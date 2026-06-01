import boto3

def lanzar_cluster():
    REGION_LABORATORIO = 'us-east-1' 
    
    emr_client = boto3.client('emr', region_name=REGION_LABORATORIO) 
    
    BUCKET_NAME = "cluster-arleen"
    RUTA_LOGS_S3 = f"s3://{BUCKET_NAME}/logs_emr/"

    respuesta = emr_client.run_job_flow(
        Name='Cluster-Hadoop-Hive',
        ReleaseLabel='emr-7.0.0',
        LogUri=RUTA_LOGS_S3,
        Applications=[
            {'Name': 'Hadoop'},
            {'Name': 'Hive'}
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Nodo Principal (Master)',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm4.large',
                    'InstanceCount': 1,
                },
                {
                    'Name': 'Nodo Central (Core)',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm4.large',
                    'InstanceCount': 2,
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName': 'MiClaveEC1',
        },
        JobFlowRole='EMR_EC2_DefaultRole',  
        ServiceRole='EMR_DefaultRole',      
        VisibleToAllUsers=True
    )

    cluster_id = respuesta['JobFlowId']
    print("\nClúster creado con éxito.")
    print(f"\nCLUSTER ID: {cluster_id}")
    return cluster_id

if __name__ == "__main__":
    lanzar_cluster()