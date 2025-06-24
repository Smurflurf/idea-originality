import requests
import json
import time
import re
import os
import hashlib
from sentence_transformers import SentenceTransformer, models as models
from qdrant_client import QdrantClient, models as qdrant_models 


def setup_vectorizer():
    MODEL_NAME = 'dwzhu/e5-base-4k'
    print(f"Loading the sentence-transformer-model: '{MODEL_NAME}'...")
    word_embedding_model = models.Transformer(MODEL_NAME)
    pooling_model = models.Pooling(word_embedding_model.get_word_embedding_dimension())
    try:
        model = SentenceTransformer(modules=[word_embedding_model, pooling_model], device='cuda')
    except: 
        model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
    return model

def encode_documents_for_db_batch(model, texts: list[str], batch_size: int) -> list[list[float]]:
    if not texts: 
        print("l24: no texts to encode.")
        return []
    prefixed_texts = ["passage: " + t for t in texts]
    vectors = model.encode(prefixed_texts, batch_size=batch_size, show_progress_bar=False)
    return vectors.tolist()

def setup_db_client(host, port):
    print(f"Connecting to Qdrant on {host}:{port}...")
    client = QdrantClient(host=host, port=port)
    print("Database-Manager connected.")
    return client

def upsert_points_to_db(client, collection_name, vectors, payloads):
    if not vectors: 
        print("l38: no vectors to upload.")
        return
    points_to_upsert = []
    for vector, payload in zip(vectors, payloads):
        original_json_obj = payload.get("original_json", {})
        deterministic_string = json.dumps(original_json_obj, sort_keys=True, separators=(',', ':'))
        hasher = hashlib.sha256()
        hasher.update(deterministic_string.encode('utf-8'))
        full_hash_bytes = hasher.digest()
        first_8_bytes = full_hash_bytes[:8]
        id_as_64_bit_int = int.from_bytes(first_8_bytes, 'big')
        points_to_upsert.append(
            qdrant_models.PointStruct(id=id_as_64_bit_int, vector=vector, payload=payload)
        )
    client.upsert(collection_name=collection_name, points=points_to_upsert, wait=True)
    print(f"{len(points_to_upsert)} Points uploaded successfully.")

def sanitize_text(text: str) -> str:
    if not text: 
        print("l57: no text to sanitize.")
        return ""
    text = text.strip()
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    text = text.replace('/', '').replace('\\', '').replace('$', '').replace('{', ' ').replace('}', ' ').replace('_', '')
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# ===================================================================
# MAIN SCRIPT
# ===================================================================

SERVER_IP = os.environ.get('QDRANT_HOST', "localhost")
API_BASE_URL = f"http://{SERVER_IP}:8000/"
QDRANT_PORT = 6333
COLLECTION_NAME = "idea-db"
PROCESSING_BATCH_SIZE = 32

model = setup_vectorizer()
db_client = setup_db_client(SERVER_IP, QDRANT_PORT)

while True:
    try:
        response = requests.get(f"{API_BASE_URL}/get_random_batch", timeout=60)
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "eof":
            print("No remaining tasks on the server. Process stopped successfully.")
            break
        
        if data.get("status") != "ok":
            print(f"Server returned error: {data.get('message')}")
            time.sleep(5)
            continue
        
        batch_to_process_index = data.get("batch_index")
        batches_left = data.get("batches_left")
        raw_lines = data.get("lines", [])
        
        if batch_to_process_index is None or not raw_lines:
            print("Server returned an invalid answer. Trying again...")
            time.sleep(5)
            continue

        print(f"Receiving and processing Task for Batch #{batch_to_process_index}. Remaining Batches: {batches_left}")

        texts_to_embed = []
        payloads = []
        for line in raw_lines:
            try:
                paper = json.loads(line)
                title, abstract = paper.get('title', ''), paper.get('abstract', '')
                if not abstract or not title: continue
                
                full_text = f"passage: {sanitize_text(title)}. {sanitize_text(abstract)}"
                texts_to_embed.append(full_text)
                payloads.append({"type": "arXiv", "original_json": paper})
            except json.JSONDecodeError:
                continue
        
        if texts_to_embed:
            vectors = encode_documents_for_db_batch(model, texts_to_embed, batch_size=PROCESSING_BATCH_SIZE)
            upsert_points_to_db(db_client, COLLECTION_NAME, vectors, payloads)
        
        requests.post(f"{API_BASE_URL}/complete_batch", json={"batch_index": batch_to_process_index})
        print(f"Success for Batch #{batch_to_process_index} sent to Server.")
            
    except requests.exceptions.RequestException as e:
        print(f"Networkerror: {e}. Wait 60 seconds and try again...")
        time.sleep(60)
    except Exception as e:
        print(f"Unexpected error in client: {e}. Continuing next Task...")
        time.sleep(10)