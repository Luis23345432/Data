import uuid
from faker import Faker
from utils.write_to_json import write_to_json

faker = Faker()

def generate_comments(tenant_ids, users, rooms, count_per_tenant):
    comments = []
    for tenant_id in tenant_ids:
        for _ in range(count_per_tenant):
            comment_id = str(uuid.uuid4())
            user = faker.random_element(users)
            room = faker.random_element(rooms)
            comments.append({
                "tenant_id": tenant_id,
                "comment_id": comment_id,
                "user_id": user["user_id"],
                "room_id": room["room_id"],
                "comment_text": faker.text(max_nb_chars=200),
                "created_at": faker.date_time_this_year().isoformat(),
            })
    write_to_json(comments, "comments")
    return comments
