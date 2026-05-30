import boto3
import time
import os

def enviar_job_hive():
    emr_client = boto3.client('emr', region_name='us-east-1')
    s3_client = boto3.client('s3', region_name='us-east-1')

    CLUSTER_ID = "j-32KH8ENUX5NSP"  
    BUCKET_NAME = "cluster-arleen"   
    
    # Ruta en HDFS donde Hive guardará los datos de la tabla 'resultado_hive_hdfs'
    ruta_hive_hdfs = "hdfs:///user/hive/warehouse/resultado_hive_hdfs"
    # Carpeta temporal en S3 para poder descargar los archivos a tu PC
    prefijo_s3_temporal = "temporal_hdfs_hive/"

    print("Preparando consulta de Hive WordCount con corrección de acentos...")

    # CONSULTA MEJORADA: TRANSLATE en lugar de REGEXP_REPLACE para igualar a Python
    consulta_hive = (
        # 1. Tabla origen (Apunta a tus archivos de texto en HDFS)
        "CREATE EXTERNAL TABLE IF NOT EXISTS libros (linea STRING) "
        "STORED AS TEXTFILE LOCATION 'hdfs:///user/hadoop/prueba/'; "
        
        # 2. Tabla destino interna en HDFS
        "CREATE TABLE IF NOT EXISTS resultado_hive_hdfs (palabra STRING, total INT) "
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'; "
        
        # 3. Inserción con traducción exacta de caracteres con tilde
        "INSERT OVERWRITE TABLE resultado_hive_hdfs "
        "SELECT LOWER(TRANSLATE(palabra_limpia, 'áéíóúü', 'aeiouu')) AS palabra, COUNT(1) AS total "
        "FROM libros "
        "LATERAL VIEW EXPLODE(SPLIT(linea, '[^a-zñA-ZÑ]+')) t AS palabra_limpia "
        "WHERE palabra_limpia != '' "
        "GROUP BY LOWER(TRANSLATE(palabra_limpia, 'áéíóúü', 'aeiouu')) "
        "ORDER BY total DESC;"
    )

    print(f"Enviando consulta de Hive como Step al clúster {CLUSTER_ID}...")

    respuesta = emr_client.add_job_flow_steps(
        JobFlowId=CLUSTER_ID,
        Steps=[
            # STEP 1: Ejecutar la consulta HiveQL que escribe en la tabla HDFS
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
            # STEP 2: Copiar la carpeta de la tabla desde HDFS hacia S3 temporal
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

    # Monitoreamos el segundo paso (el de extracción a S3)
    step_extrac_id = respuesta['StepIds'][1]
    print(f"⏳ Procesando en EMR. Monitoreando paso de descarga: {step_extrac_id}")

    # Bucle de espera hasta que AWS termine de trabajar
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

    # STEP 3: Descargar los archivos finales desde S3 a tu máquina local
    print("📥 Descargando resultados de Hive a tu máquina local...")
    carpeta_local = "./resultados_hive_hdfs"
    os.makedirs(carpeta_local, exist_ok=True)
    
    objetos = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefijo_s3_temporal)
    
    if 'Contents' in objetos:
        for obj in objetos['Contents']:
            nombre_archivo = obj['Key'].split('/')[-1]
            
            # Descargar solo archivos de datos reales ignorando archivos ocultos
            if nombre_archivo and not nombre_archivo.startswith('.'):
                ruta_final_local = os.path.join(carpeta_local, nombre_archivo)
                s3_client.download_file(BUCKET_NAME, obj['Key'], ruta_final_local)
                print(f"✅ Descargado: {ruta_final_local}")
                
    print(f"✨ ¡Todo listo! Tus resultados de Hive corregidos están en: {carpeta_local}")

if __name__ == "__main__":
    enviar_job_hive()