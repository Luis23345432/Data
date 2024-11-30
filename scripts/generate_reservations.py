import uuid
from faker import Faker
from utils.write_to_json import write_to_json

faker = Faker()

def generate_reservations(tenant_ids, users, rooms, services, count_per_tenant):
    reservations = []
    for tenant_id in tenant_ids:
        for _ in range(count_per_tenant):
            reservation_id = str(uuid.uuid4())
            user = faker.random_element(users)
            room = faker.random_element(rooms)
            service_sample = faker.random_sample(services, length=3)
            reservations.append({
                "tenant_id": tenant_id,
                "reservation_id": reservation_id,
                "user_id": user["user_id"],
                "room_id": room["room_id"],
                "service_ids": [s["service_id"] for s in service_sample],
                "start_date": faker.date_this_year().isoformat(),
                "end_date": faker.date_this_year().isoformat(),
                "status": "confirmed",
            })
    write_to_json(reservations, "reservations")
    return reservations
