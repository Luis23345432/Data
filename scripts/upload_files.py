import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

s3 = boto3.client("s3", region_name="us-east-1")

def upload_files_to_s3(bucket_name):
    for root, _, files in os.walk("output"):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                try:
                    print(f"Subiendo {file} a {bucket_name}...")
                    s3.upload_file(file_path, bucket_name, file)
                    print(f"Archivo {file} subido con éxito.")
                except NoCredentialsError:
                    print("Error: No se encontraron credenciales de AWS.")
                    # Aquí puedes agregar lógica adicional, como pedir las credenciales
                    break  # O podrías continuar dependiendo de lo que quieras hacer
                except PartialCredentialsError:
                    print("Error: Las credenciales de AWS están incompletas.")
                    # Aquí puedes manejar la falta de una parte de las credenciales
                    break  # O podrías continuar dependiendo de lo que quieras hacer
                except Exception as e:
                    print(f"Error inesperado al subir el archivo {file}: {str(e)}")
                    # Podrías agregar más lógica de manejo de errores si es necesario

if __name__ == "__main__":
    bucket_name = "nuevo-hotel-data-bucket"  # Asegúrate de que este nombre sea correcto
    upload_files_to_s3(bucket_name)
