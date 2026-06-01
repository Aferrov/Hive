import boto3
import os

def generar_llave_aws():
    ec2_client = boto3.client('ec2', region_name='us-east-1')

    nombre_llave = "MiClaveEC1"
    nombre_archivo_local = f"{nombre_llave}.pem"

    try:
        respuesta = ec2_client.create_key_pair(
            KeyName=nombre_llave,
            KeyType='rsa'
        )

        contenido_pem = respuesta['KeyMaterial']

        with open(nombre_archivo_local, "w", encoding="utf-8") as f:
            f.write(contenido_pem)

        os.chmod(nombre_archivo_local, 0o400)

        print(f"Llave creada exitosamente en AWS")

    except ec2_client.exceptions.ClientError as e:
        if "InvalidKeyPair.Duplicate" in str(e):
            print(f"La llave '{nombre_llave}' ya existe en tu cuenta de AWS de esta región.")
        else:
            print(f"Ocurrió un error con AWS: {e}")

if __name__ == "__main__":
    generar_llave_aws()