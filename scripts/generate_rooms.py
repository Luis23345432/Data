import uuid
from faker import Faker
from utils.write_to_json import write_to_json
import random

faker = Faker()

# Lista de URLs de imágenes (puedes agregar más URLs aquí)
image_urls = [
    "https://elcomercio.pe/resizer/pfr9uvs5EojbKKmnLQfL27eiT4w=/1200x900/smart/filters:format(jpeg):quality(75)/arc-anglerfish-arc2-prod-elcomercio.s3.amazonaws.com/public/JLLIVKJJOBAFJCW7FPOYTICIBQ.jpg",
    "https://portal.andina.pe/EDPfotografia/Thumbnail/2015/06/15/000298514W.jpg",
    "https://static1.eskypartners.com/travelguide/vancouver-hotels.jpg",
    "https://www.ahstatic.com/photos/b464_roq2bh_00_p_1024x768.jpg",
    "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRoR_9M3VonT3_MXtrWAGjS5S3P7_YltJ3rwA&s",
    "https://procoen.com/wp-content/uploads/2019/11/control-de-humedad-en-habitaciones-de-hotel-scaled.jpg",
    "https://www.ahstatic.com/photos/b464_rokgb_00_p_1024x768.jpg",
    "https://upload.wikimedia.org/wikipedia/commons/5/56/2008-1227-EncoreLV-002-Pan.JPG",
    "https://s3.amazonaws.com/arc-wordpress-client-uploads/infobae-wp/wp-content/uploads/2019/05/20152644/The-Resort-at-Pedregal-1.jpg",
    "https://imageio.forbes.com/specials-images/imageserve/65624c037b711654a628a642/The-bedroom-at-andBeyond-Punakha-River-Lodge-in-Bhutan-is-decorated-in-traditional/960x0.jpg?format=jpg&width=1440",
    "https://images.trvl-media.com/lodging/4000000/3050000/3045500/3045476/8cd61a2c.jpg?impolicy=resizecrop&rw=575&rh=575&ra=fill",
    "https://www.kayak.cl/rimg/himg/73/73/b8/expediav2-317862-160740-398450.jpg?width=968&height=607&crop=true",
    "https://www.infobae.com/resizer/v2/https%3A%2F%2Fs3.amazonaws.com%2Farc-wordpress-client-uploads%2Finfobae-wp%2Fwp-content%2Fuploads%2F2019%2F05%2F20152130%2FBulgari-Resort-Bali-2.jpg?auth=22c9da425bfca090b5cc6d767655bdd430edb5513f1fcf7a8592acfe9ef6efbb&smart=true&width=350&height=197&quality=85",
    "https://y.cdrst.com/foto/hotel-sf/2809/granderesp/banyan-tree-bangkok-general-122311bb.jpg"
]


def generate_rooms(tenant_ids, count_per_tenant):
    rooms = []
    for tenant_id in tenant_ids:
        for _ in range(count_per_tenant):
            room_id = str(uuid.uuid4())

            # Generamos los datos de la habitación
            room = {
                "tenant_id": tenant_id,
                "room_id": room_id,
                "room_name": faker.word(),
                "max_persons": faker.random_int(min=1, max=6),
                "room_type": faker.random_element(["Single", "Double", "Suite"]),
                "price_per_night": faker.random_int(min=50, max=500),
                "description": faker.text(max_nb_chars=200),
                "availability": "disponible",
                "created_at": faker.date_time_this_year().isoformat(),
                "image": random.choice(image_urls)  # Seleccionamos una URL aleatoria
            }

            # Agregamos la habitación a la lista
            rooms.append(room)

    # Guardamos los datos generados en formato JSON
    write_to_json(rooms, "rooms")
    return rooms
