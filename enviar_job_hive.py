import boto3
import time
import os

def enviar_job_hive():
    emr_client = boto3.client('emr', region_name='us-east-1')
    s3_client = boto3.client('s3', region_name='us-east-1')

    CLUSTER_ID = "j-2QCK2QFWR1LCL"  
    BUCKET_NAME = "cluster-arleen"   
    
    ruta_hive_hdfs = "hdfs:///user/hive/warehouse/resultado_hive_hdfs"
    prefijo_s3_temporal = "temporal_hdfs_hive/"

    consulta_hive = (
        "DROP TABLE IF EXISTS libros; "
        "CREATE EXTERNAL TABLE libros (linea STRING) "
        "STORED AS TEXTFILE LOCATION 'hdfs:///user/hadoop/prueba/'; "
        
        "DROP TABLE IF EXISTS resultado_hive_hdfs; "
        "CREATE TABLE resultado_hive_hdfs (palabra STRING, total INT) "
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'; "
        
        "INSERT OVERWRITE TABLE resultado_hive_hdfs "
        "SELECT LOWER(TRANSLATE(palabra_limpia, 'áéíóúü', 'aeiouu')) AS palabra, COUNT(1) AS total "
        "FROM libros "
        "LATERAL VIEW EXPLODE(SPLIT(linea, '[^a-zñA-ZÑ]+')) t AS palabra_limpia "
        "WHERE palabra_limpia != '' "
        "GROUP BY LOWER(TRANSLATE(palabra_limpia, 'áéíóúü', 'aeiouu')) "
        "ORDER BY total DESC;"
    )

    print(f"Enviando consulta de Hive al clúster {CLUSTER_ID}")

    respuesta = emr_client.add_job_flow_steps(
        JobFlowId=CLUSTER_ID,
        Steps=[
            
            {
                'Name': 'WordCount_Hive_Tablas_HDFS',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'hive',
                        '-e',
                        consulta_hive
                    ]
                }
            },
            
            {
                'Name': 'Exportar_Hive_HDFS_a_S3_Temporal',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'bash', '-c', 
                        f'hdfs dfs -get {ruta_hive_hdfs} /tmp/salida_hive_local && aws s3 cp /tmp/salida_hive_local s3://{BUCKET_NAME}/{prefijo_s3_temporal} --recursive && rm -rf /tmp/salida_hive_local'
                    ]
                }
            }
        ]
    )

    step_extrac_id = respuesta['StepIds'][1]
    
    while True:
        descripcion = emr_client.describe_step(ClusterId=CLUSTER_ID, StepId=step_extrac_id)
        estado = Boston = descripcion['Step']['Status']['State']
        
        if estado == 'COMPLETED':
            print("¡Procesamiento en HDFS y copia intermedia finalizados con éxito!")
            break
        elif estado in ['FAILED', 'CANCELLED']:
            print(f"❌ El proceso falló o fue cancelado en EMR con estado: {estado}")
            return
        
        time.sleep(15)

    carpeta_local = "./resultados_hive_hdfs"
    os.makedirs(carpeta_local, exist_ok=True)
    
    objetos = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefijo_s3_temporal)
    
    if 'Contents' in objetos:
        for obj in objetos['Contents']:
            nombre_archivo = obj['Key'].split('/')[-1]
            
            if nombre_archivo and not nombre_archivo.startswith('.'):
                ruta_final_local = os.path.join(carpeta_local, nombre_archivo)
                s3_client.download_file(BUCKET_NAME, obj['Key'], ruta_final_local)
                
    
if __name__ == "__main__":
    enviar_job_hive()