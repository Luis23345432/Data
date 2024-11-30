import json
import os

def write_to_json(data_list, filename):
    """Escribe los datos en formato JSON en la carpeta de salida."""
    file_path = f"output/{filename}.json"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, mode="w", encoding="utf-8") as file:
        json.dump(data_list, file, ensure_ascii=False, indent=2)
