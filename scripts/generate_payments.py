import uuid
from faker import Faker
from utils.write_to_json import write_to_json

faker = Faker()

def generate_payments(tenant_ids, reservations, count_per_tenant):
    payments = []
    for tenant_id in tenant_ids:
        for _ in range(count_per_tenant):
            payment_id = str(uuid.uuid4())
            reservation = faker.random_element(reservations)
            payments.append({
                "tenant_id": tenant_id,
                "payment_id": payment_id,
                "reservation_id": reservation["reservation_id"],
                "monto_pago": faker.random_int(min=100, max=2000),
                "created_at": faker.date_time_this_year().isoformat(),
                "status": "completed",
            })
    write_to_json(payments, "payments")
    return payments
