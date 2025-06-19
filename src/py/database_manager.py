from qdrant_client import QdrantClient, models
import os
import hashlib
import json

SERVER_IP = os.environ.get('QDRANT_HOST', "localhost")
QDRANT_PORT = int(os.getenv('QDRANT_PORT', 6333))
COLLECTION_NAME = os.getenv('QDRANT_COLLECTION_NAME', "idea-db") 

client = QdrantClient(host=SERVER_IP, port=QDRANT_PORT)

def ensure_collection_exists():
    """
    Stellt sicher, dass die Collection existiert. Erstellt sie, falls nicht.
    """
    try:
        client.get_collection(collection_name=COLLECTION_NAME)
    except Exception:
        print(f"Erstelle Collection '{COLLECTION_NAME}'...")
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=models.VectorParams(size=768, distance=models.Distance.COSINE)
        )
        print("Collection erstellt.")

def upsert_points(vectors: list[list[float]], payloads: list[dict]):
    if not vectors:
        return

    points_to_upsert = []
    for i, (vector, payload) in enumerate(zip(vectors, payloads)):
        text_to_hash = payload.get("original_json", {})
        text_to_hash = json.dumps(text_to_hash, sort_keys=True, separators=(',', ':'))
        hasher = hashlib.sha256()
        hasher.update(text_to_hash.encode('utf-8'))
        full_hash_bytes = hasher.digest()
        first_8_bytes = full_hash_bytes[:8]
        deterministic_id_64bit = int.from_bytes(first_8_bytes, 'big')

        points_to_upsert.append(
            models.PointStruct(
                id=deterministic_id_64bit,
                vector=vector,
                payload=payload
            )
        )
    
    client.upsert(
        collection_name=COLLECTION_NAME,
        points=points_to_upsert,
        wait=False
    )
    print(f"{len(points_to_upsert)} Punkte erfolgreich zum Upload in die Warteschlange gestellt.")