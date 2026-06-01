import boto3
import time
import os

def enviar_job_taxi():
    emr_client = boto3.client('emr', region_name='us-east-1')
    s3_client  = boto3.client('s3',  region_name='us-east-1')

    CLUSTER_ID  = "j-2QCK2QFWR1LCL"
    BUCKET_NAME = "cluster-arleen"

    ruta_warehouse_hdfs = "hdfs:///user/hive/warehouse"
    prefijo_s3_temporal = "resultados_nyc_taxi/"

    consulta_hive = (
        "DROP TABLE IF EXISTS nyc_taxi_parquet; "
        "CREATE EXTERNAL TABLE nyc_taxi_parquet ("
        "  VendorID INT, "
        "  tpep_pickup_datetime STRING, "
        "  tpep_dropoff_datetime STRING, "
        "  passenger_count INT, trip_distance DOUBLE, RatecodeID INT, "
        "  store_and_fwd_flag STRING, PULocationID INT, DOLocationID INT, "
        "  payment_type INT, fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, "
        "  tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE, "
        "  total_amount DOUBLE, congestion_surcharge DOUBLE"
        ") STORED AS PARQUET "
        "LOCATION 'hdfs:///user/hadoop/datos_taxi/'; "

        "DROP TABLE IF EXISTS nyc_taxi; "
        "CREATE TABLE nyc_taxi ("
        "  vendorid INT, "
        "  fecha_recogida STRING, "
        "  fecha_entrega STRING, "
        "  passenger_count INT, "
        "  trip_distance DOUBLE, "
        "  payment_type INT, "
        "  total_amount DOUBLE, "
        "  hora_recogida INT "
        ") PARTITIONED BY (year STRING, month STRING) "
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' "
        "STORED AS TEXTFILE; "

        "SET hive.exec.dynamic.partition=true; "
        "SET hive.exec.dynamic.partition.mode=nonstrict; "
        "SET hive.exec.max.dynamic.partitions=2000; "
        "SET hive.exec.max.dynamic.partitions.pernode=2000; "

        # INSERT DEFINITIVO: Recorte estricto de strings a 10 dígitos para sincronizar el calendario real
        "INSERT OVERWRITE TABLE nyc_taxi PARTITION(year, month) "
        "SELECT "
        "  VendorID, "
        "  FROM_UNIXTIME(CAST(SUBSTR(tpep_pickup_datetime, 1, 10) AS BIGINT)) AS fecha_recogida, "
        "  FROM_UNIXTIME(CAST(SUBSTR(tpep_dropoff_datetime, 1, 10) AS BIGINT)) AS fecha_entrega, "
        "  passenger_count, "
        "  trip_distance, "
        "  payment_type, "
        "  total_amount, "
        "  HOUR(CAST(FROM_UNIXTIME(CAST(SUBSTR(tpep_pickup_datetime, 1, 10) AS BIGINT)) AS STRING)) AS hora_recogida, "
        "  SUBSTR(CAST(FROM_UNIXTIME(CAST(SUBSTR(tpep_pickup_datetime, 1, 10) AS BIGINT)) AS STRING), 1, 4) AS year, "
        "  SUBSTR(CAST(FROM_UNIXTIME(CAST(SUBSTR(tpep_pickup_datetime, 1, 10) AS BIGINT)) AS STRING), 6, 2) AS month "
        "FROM nyc_taxi_parquet "
        "WHERE tpep_pickup_datetime IS NOT NULL "
        "  AND trip_distance > 0 "
        "  AND total_amount  > 0; "

        "DROP TABLE IF EXISTS viajes; "
        "CREATE TABLE viajes "
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' "
        "AS "
        "SELECT "
        "  COUNT(1)                     AS total_viajes, "
        "  ROUND(AVG(trip_distance), 2) AS promedio_distancia, "
        "  ROUND(AVG(total_amount),  2) AS promedio_tarifa, "
        "  ROUND(SUM(total_amount),  2) AS ingreso_total "
        "FROM nyc_taxi; "

        "DROP TABLE IF EXISTS horas_trafico; "
        "CREATE TABLE horas_trafico "
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' "
        "AS "
        "SELECT "
        "  hora_recogida, "
        "  COUNT(1)                    AS cantidad_viajes, "
        "  ROUND(AVG(total_amount), 2) AS promedio_tarifa "
        "FROM nyc_taxi "
        "GROUP BY hora_recogida "
        "ORDER BY cantidad_viajes DESC; "

        "DROP TABLE IF EXISTS metodos_pago; "
        "CREATE TABLE metodos_pago "
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' "
        "AS "
        "SELECT "
        "  payment_type, "
        "  COUNT(1)                     AS uso_total, "
        "  ROUND(COUNT(1) * 100.0 / SUM(COUNT(1)) OVER (), 2) AS porcentaje "
        "FROM nyc_taxi "
        "GROUP BY payment_type "
        "ORDER BY uso_total DESC; "

        "DROP TABLE IF EXISTS viajes_costosos; "
        "CREATE TABLE viajes_costosos "
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' "
        "AS "
        "SELECT "
        "  fecha_recogida, "
        "  fecha_entrega, "
        "  passenger_count, "
        "  ROUND(trip_distance, 2) AS distancia_millas, "
        "  ROUND(total_amount,  2) AS total_usd "
        "FROM nyc_taxi "
        "ORDER BY total_amount DESC "
        "LIMIT 10; "

        "DROP TABLE IF EXISTS reporte; "
        "CREATE TABLE reporte "
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' "
        "AS "
        "SELECT hora_recogida, AVG(total_amount) AS promedio_pago "
        "FROM nyc_taxi "
        "WHERE year = '2026' " 
        "GROUP BY hora_recogida;"
    )

    comando_exportacion = (
        f"mkdir -p /tmp/salida_taxi && "
        f"hdfs dfs -get {ruta_warehouse_hdfs}/viajes          /tmp/salida_taxi/viajes          && "
        f"hdfs dfs -get {ruta_warehouse_hdfs}/horas_trafico   /tmp/salida_taxi/horas_trafico   && "
        f"hdfs dfs -get {ruta_warehouse_hdfs}/metodos_pago    /tmp/salida_taxi/metodos_pago    && "
        f"hdfs dfs -get {ruta_warehouse_hdfs}/viajes_costosos /tmp/salida_taxi/viajes_costosos && "
        f"hdfs dfs -get {ruta_warehouse_hdfs}/reporte         /tmp/salida_taxi/reporte         && "
        f"aws s3 cp /tmp/salida_taxi s3://{BUCKET_NAME}/{prefijo_s3_temporal} --recursive && "
        f"rm -rf /tmp/salida_taxi"
    )

    print(f"Enviando job Hive al clúster {CLUSTER_ID}...")

    respuesta = emr_client.add_job_flow_steps(
        JobFlowId=CLUSTER_ID,
        Steps=[
            {
                'Name': 'NYC_Taxi_Consultas_Hive',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'hive',
                        '-hiveconf', 'parquet.column.index.access=true',
                        '-hiveconf', 'hive.case.sensitive.lookup=false',
                        '-e', consulta_hive
                    ]
                }
            },
            {
                'Name': 'Exportar_HDFS_a_S3',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['bash', '-c', comando_exportacion]
                }
            }
        ]
    )

    step_exportar_id = respuesta['StepIds'][1]
    
    while True:
        estado = emr_client.describe_step(
            ClusterId=CLUSTER_ID,
            StepId=step_exportar_id
        )['Step']['Status']['State']

        if estado == 'COMPLETED':
            break
        elif estado in ('FAILED', 'CANCELLED', 'INTERRUPTED'):
            return

        time.sleep(30)

    carpeta_local = "./resultados_nyc_taxi"
    os.makedirs(carpeta_local, exist_ok=True)

    objetos = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefijo_s3_temporal)

    if 'Contents' not in objetos:
        return

    for obj in objetos['Contents']:
        nombre_archivo = obj['Key'].split('/')[-1]
        if not nombre_archivo or nombre_archivo.startswith('_') or nombre_archivo.startswith('.'):
            continue

        ruta_relative = obj['Key'].replace(prefijo_s3_temporal, "")
        ruta_final    = os.path.join(carpeta_local, ruta_relative)
        os.makedirs(os.path.dirname(ruta_final), exist_ok=True)

        s3_client.download_file(BUCKET_NAME, obj['Key'], ruta_final)


if __name__ == "__main__":
    enviar_job_taxi()