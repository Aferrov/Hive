import boto3
import time
import os

def enviar_job_hadoop():
    emr_client = boto3.client('emr', region_name='us-east-1')
    s3_client = boto3.client('s3', region_name='us-east-1')

    CLUSTER_ID = "j-32KH8ENUX5NSP"  
    BUCKET_NAME = "cluster-arleen"   

    # Rutas de almacenamiento
    ruta_salida_hdfs = "hdfs:///user/hadoop/salida_mapreduce2"
    prefijo_s3_temporal = "temporal_hdfs_hadoop/"

    print("🚀 Enviando tarea de Hadoop MapReduce a EMR...")

    respuesta = emr_client.add_job_flow_steps(
        JobFlowId=CLUSTER_ID,
        Steps=[
            # STEP 1: Ejecución del MapReduce usando tus scripts reales
            {
                'Name': 'WordCount_Hadoop_HDFS',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'hadoop-streaming',
                        # 1. CORRECCIÓN: Apuntar a tus archivos reales subidos a S3
                        '-files', f's3://{BUCKET_NAME}/scripts/wordcount_mapper.py,s3://{BUCKET_NAME}/scripts/wordcount_reducer.py',
                        # 2. CORRECCIÓN: Indicarle a Hadoop que ejecute TUS scripts corregidos
                        '-mapper', 'python3 wordcount_mapper.py',
                        '-reducer', 'python3 wordcount_reducer.py',
                        '-input', 'hdfs:///user/hadoop/nuevos_textos/El_Conde_Montecristo.txt',
                        '-output', ruta_salida_hdfs
                    ]
                }
            },
            # STEP 2: Extraer del HDFS del clúster a S3 temporal para poder bajarlo
            {
                'Name': 'Exportar_Hadoop_HDFS_a_S3',
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

    # Monitorear el paso de extracción (el segundo Step)
    step_extrac_id = respuesta['StepIds'][1]
    
    # Bucle de espera activo
    while True:
        descripcion = emr_client.describe_step(ClusterId=CLUSTER_ID, StepId=step_extrac_id)
        estado = descripcion['Step']['Status']['State']
        
        if estado == 'COMPLETED':
            print("¡Procesamiento en HDFS exitoso y copiado a S3!")
            break
        elif estado in ['FAILED', 'CANCELLED']:
            print(f"❌ El proceso falló en EMR con estado: {estado}")
            return
        
        time.sleep(15)

    # STEP 3: Descarga física de los archivos desde S3 a tu máquina local
    print("📥 Descargando resultados de Hadoop a tu máquina local...")
    carpeta_local = "./resultados_hadoop_hdfs"
    os.makedirs(carpeta_local, exist_ok=True)
    
    objetos = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefijo_s3_temporal)
    
    if 'Contents' in objetos:
        for obj in objetos['Contents']:
            nombre_archivo = obj['Key'].split('/')[-1]
            
            # Descargar los archivos de datos (part-00000, part-00001, etc.)
            if nombre_archivo.startswith('part-'):
                ruta_final_local = os.path.join(carpeta_local, nombre_archivo)
                s3_client.download_file(BUCKET_NAME, obj['Key'], ruta_final_local)
                print(f"✅ Descargado: {ruta_final_local}")
                
    print("✨ ¡Todo listo! Tus resultados de MapReduce están en la carpeta './resultados_hadoop_hdfs'")

if __name__ == "__main__":
    enviar_job_hadoop()