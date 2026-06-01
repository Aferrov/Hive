import boto3
import time
import os

def enviar_indice_invertido_hive():
    emr_client = boto3.client('emr', region_name='us-east-1')
    s3_client = boto3.client('s3', region_name='us-east-1')

    CLUSTER_ID = "j-2QCK2QFWR1LCL"  
    BUCKET_NAME = "cluster-arleen"   
    
    # Ruta en HDFS donde Hive guardará la tabla final del índice
    ruta_hive_hdfs = "hdfs:///user/hive/warehouse/resultado_indice_invertido"
    prefijo_s3_temporal = "temporal_hdfs_indice/"

    consulta_hive = (
        "DROP TABLE IF EXISTS libros; "
        "CREATE EXTERNAL TABLE libros (linea STRING) "
        "STORED AS TEXTFILE LOCATION 'hdfs:///user/hadoop/prueba/'; "
        
        "DROP TABLE IF EXISTS resultado_indice_invertido; "
        "CREATE TABLE resultado_indice_invertido (palabra STRING, documentos STRING) "
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'; "
        
        "INSERT OVERWRITE TABLE resultado_indice_invertido "
        "SELECT "
        "   TRANSLATE(palabra_limpia, 'áéíóúü', 'aeiouu') AS palabra, "
        "   CONCAT_WS(', ', COLLECT_SET(nombre_archivo)) AS documentos "
        "FROM ( "
        "   SELECT "
        "       linea, "
        "       REGEXP_EXTRACT(INPUT__FILE__NAME, '.*/([^/]+)$', 1) AS nombre_archivo "
        "   FROM libros "
        ") origen "
        "LATERAL VIEW EXPLODE(SPLIT(LOWER(linea), '[^a-zñ]+')) t AS palabra_limpia "
        "WHERE palabra_limpia != '' "
        "GROUP BY TRANSLATE(palabra_limpia, 'áéíóúü', 'aeiouu') "
        "ORDER BY palabra ASC;"
    )

    print(f"Enviando Índice Invertido de Hive al clúster {CLUSTER_ID}...")

    respuesta = emr_client.add_job_flow_steps(
        JobFlowId=CLUSTER_ID,
        Steps=[
            {
                'Name': 'Indice_Invertido_Hive_HDFS',
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
                'Name': 'Exportar_Indice_HDFS_a_S3',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'bash', '-c', 
                        f'hdfs dfs -get {ruta_hive_hdfs} /tmp/salida_indice_local && aws s3 cp /tmp/salida_indice_local s3://{BUCKET_NAME}/{prefijo_s3_temporal} --recursive && rm -rf /tmp/salida_indice_local'
                    ]
                }
            }
        ]
    )

    step_extrac_id = respuesta['StepIds'][1]


    carpeta_local = "./resultados_indice_hive"
    os.makedirs(carpeta_local, exist_ok=True)
    
    objetos = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefijo_s3_temporal)
    
    if 'Contents' in objetos:
        for obj in objetos['Contents']:
            nombre_archivo = obj['Key'].split('/')[-1]
            if nombre_archivo and not nombre_archivo.startswith('.'):
                ruta_final_local = os.path.join(carpeta_local, nombre_archivo)
                s3_client.download_file(BUCKET_NAME, obj['Key'], ruta_final_local)
                
    
if __name__ == "__main__":
    enviar_indice_invertido_hive()