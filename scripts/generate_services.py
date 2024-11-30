import uuid
from faker import Faker
from utils.write_to_json import write_to_json

faker = Faker()

def generate_services(tenant_ids, count_per_tenant):
    services = []
    for tenant_id in tenant_ids:
        for _ in range(count_per_tenant):
            service_id = str(uuid.uuid4())
            services.append({
                "tenant_id": tenant_id,
                "service_id": service_id,
                "service_category": faker.random_element(["Spa", "Gym", "Pool"]),
                "service_name": faker.word(),
                "descripcion": faker.text(max_nb_chars=200),
                "precio": faker.random_int(min=10, max=200),
            })
    write_to_json(services, "services")
    return services
