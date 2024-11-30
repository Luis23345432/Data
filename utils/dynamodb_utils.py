import boto3
import time
import json
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Configuración de clientes DynamoDB y S3
dynamodb = boto3.client("dynamodb", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

# Diccionario de mapeo de tablas y archivos en S3
tablas_archivos = {
    "dev-hotel-users": ["users.json"],
    "dev-hotel-rooms": ["rooms.json"],
    "dev-hotel-services": ["services.json"],
    "dev-hotel-reservations": ["reservations.json"],
    "dev-hotel-payments": ["payments.json"],
    "dev-hotel-comments": ["comments.json"],
}

def descargar_archivo_s3(bucket, key):
    """Descarga un archivo de S3 y lo convierte en una lista de diccionarios."""
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        contenido = response['Body'].read().decode('utf-8')
        return json.loads(contenido)
    except Exception as e:
        print(f"Error al descargar {key} de S3: {e}")
        raise

def batch_write_to_dynamodb(table_name, items, max_retries=5):
    """Inserta datos en DynamoDB en lotes de 25 elementos."""
    lotes = [items[i:i + 25] for i in range(0, len(items), 25)]
    for lote in lotes:
        request_items = {table_name: [{"PutRequest": {"Item": format_dynamodb_item(item)}} for item in lote]}
        retry_count = 0

        while retry_count < max_retries:
            try:
                response = dynamodb.batch_write_item(RequestItems=request_items)
                if 'UnprocessedItems' not in response or not response['UnprocessedItems']:
                    break
            except Exception as e:
                print(f"Error al procesar batch_write_item: {e}")
                break

            if 'UnprocessedItems' in response and response['UnprocessedItems']:
                request_items = response['UnprocessedItems']
                time.sleep(2 ** retry_count)
                retry_count += 1

def format_dynamodb_item(item):
    """Formatea el item de acuerdo con los tipos de datos requeridos por DynamoDB."""
    formatted_item = {}
    for key, value in item.items():
        if isinstance(value, str):
            formatted_item[key] = {"S": value}  # Para cadenas de texto
        elif isinstance(value, int):
            formatted_item[key] = {"N": str(value)}  # Para números, convertir a cadena
        elif isinstance(value, bool):
            formatted_item[key] = {"BOOL": value}  # Para booleanos
        else:
            formatted_item[key] = {"S": str(value)}  # Default a cadena si no está definido
    return formatted_item

def upload_files_to_s3(bucket_name):
    """Sube los archivos generados a S3."""
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
                    break  # O podrías continuar dependiendo de lo que quieras hacer
                except PartialCredentialsError:
                    print("Error: Las credenciales de AWS están incompletas.")
                    break
                except Exception as e:
                    print(f"Error inesperado al subir el archivo {file}: {str(e)}")

def process_and_upload_to_dynamodb(bucket_name):
    """Procesa los archivos desde S3 y los sube a DynamoDB."""
    for table_name, files in tablas_archivos.items():
        for file_name in files:
            print(f"Descargando {file_name} desde S3...")
            items = descargar_archivo_s3(bucket_name, file_name)
            print(f"Insertando en la tabla {table_name}...")
            batch_write_to_dynamodb(table_name, items)
            print(f"Datos insertados en {table_name} desde {file_name}.")
