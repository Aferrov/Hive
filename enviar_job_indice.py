import boto3
import time
import os

def enviar_job_hadoop():
    emr_client = boto3.client('emr', region_name='us-east-1')
    s3_client = boto3.client('s3', region_name='us-east-1')

    CLUSTER_ID = "j-2QCK2QFWR1LCL"  
    BUCKET_NAME = "cluster-arleen"   

    # Rutas de almacenamiento
    ruta_salida_hdfs = "hdfs:///user/hadoop/salida_indice"
    prefijo_s3_temporal = "temporal_hdfs_indice/"

    
    respuesta = emr_client.add_job_flow_steps(
        JobFlowId=CLUSTER_ID,
        Steps=[
            {
                'Name': 'Indice_Invertido_HDFS',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'hadoop-streaming',
                        '-files', f's3://{BUCKET_NAME}/scripts/indice_mapper.py,s3://{BUCKET_NAME}/scripts/indice_reducer.py',
                        '-mapper', 'python3 indice_mapper.py',
                        '-reducer', 'python3 indice_reducer.py',
                        '-input', 'hdfs:///user/hadoop/prueba/',
                        '-output', ruta_salida_hdfs
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
                        f'hdfs dfs -get {ruta_salida_hdfs} /tmp/salida_hadoop_local && aws s3 cp /tmp/salida_hadoop_local s3://{BUCKET_NAME}/{prefijo_s3_temporal} --recursive && rm -rf /tmp/salida_hadoop_local'
                    ]
                }
            }
        ]
    )

    print(f"Tareas enviadas al cluster")

    # Monitorear el paso de extracción (el segundo Step)
    step_extrac_id = respuesta['StepIds'][1]
    
    carpeta_local = "./resultados_hadoop_hdfs_indice"
    os.makedirs(carpeta_local, exist_ok=True)
    
    objetos = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefijo_s3_temporal)
    
    if 'Contents' in objetos:
        for obj in objetos['Contents']:
            nombre_archivo = obj['Key'].split('/')[-1]
            
            if nombre_archivo.startswith('part-'):
                ruta_final_local = os.path.join(carpeta_local, nombre_archivo)
                s3_client.download_file(BUCKET_NAME, obj['Key'], ruta_final_local)
                
    
if __name__ == "__main__":
    enviar_job_hadoop()