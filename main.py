from scripts.generate_users import generate_users
from scripts.generate_rooms import generate_rooms
from scripts.generate_services import generate_services
from scripts.generate_reservations import generate_reservations
from scripts.generate_payments import generate_payments
from scripts.generate_comments import generate_comments
from scripts.upload_files import upload_files_to_s3
from utils.dynamodb_utils import batch_write_to_dynamodb, tablas_archivos, descargar_archivo_s3

if __name__ == "__main__":
    tenant_ids = ["Hotel1", "Hotel2", "Hotel3"]

    # Step 1: Generate data
    users = generate_users(tenant_ids, 3334)
    rooms = generate_rooms(tenant_ids, 3334)
    services = generate_services(tenant_ids, 3334)
    reservations = generate_reservations(tenant_ids, users, rooms, services, 3334)
    generate_payments(tenant_ids, reservations, 3334)
    generate_comments(tenant_ids, users, rooms, 3334)

    # Step 2: Upload to S3
    upload_files_to_s3("nuevo-hotel-data-bucket")

    # Step 3: Insert data into DynamoDB
    for table_name, files in tablas_archivos.items():
        for file_name in files:
            print(f"Descargando {file_name} desde S3...")
            items = descargar_archivo_s3("nuevo-hotel-data-bucket", file_name)
            print(f"Insertando en la tabla {table_name}...")
            batch_write_to_dynamodb(table_name, items)
            print(f"Datos insertados en {table_name} desde {file_name}.")
