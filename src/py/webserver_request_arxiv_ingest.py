import requests
import json
import time
import re
import hashlib
from sentence_transformers import SentenceTransformer, models as models
from qdrant_client import QdrantClient, models as nodles

def setup_vectorizer():
    MODEL_NAME = 'dwzhu/e5-base-4k'
    print(f"Lade das Sentence-Transformer-Modell: '{MODEL_NAME}'...")
    word_embedding_model = models.Transformer(MODEL_NAME)
    pooling_model = models.Pooling(word_embedding_model.get_word_embedding_dimension())
    model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
    return model

def encode_documents_for_db_batch(model, texts: list[str], batch_size: int) -> list[list[float]]:
    if not texts: return []
    prefixed_texts = ["passage: " + t for t in texts]
    vectors = model.encode(prefixed_texts, batch_size=batch_size, show_progress_bar=False)
    return vectors.tolist()

def setup_db_client(host, port):
    print(f"Verbinde mit Qdrant auf {host}:{port}...")
    client = QdrantClient(host=host, port=port)
    print("Datenbank-Manager verbunden.")
    return client

def upsert_points_to_db(client, collection_name, vectors, payloads):
    if not vectors: return
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
            nodles.PointStruct(id=id_as_64_bit_int, vector=vector, payload=payload)
        )
    client.upsert(collection_name=collection_name, points=points_to_upsert, wait=True)
    print(f"{len(points_to_upsert)} Punkte erfolgreich hochgeladen.")

def sanitize_text(text: str) -> str:
    if not text: return ""
    text = text.strip()
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    text = text.replace('/', '').replace('\\', '').replace('$', '').replace('{', ' ').replace('}', ' ').replace('_', '')
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# ===================================================================
# HAUPTSKRIPT
# ===================================================================

SERVER_IP = "localhost"
API_BASE_URL = f"http://{SERVER_IP}:8000/"
QDRANT_PORT = 6333
COLLECTION_NAME = "idea-db"
PROCESSING_BATCH_SIZE = 32

model = setup_vectorizer()
db_client = setup_db_client(SERVER_IP, QDRANT_PORT)

try:
    print("Frage Startpunkt vom Server ab...")
    progress_response = requests.get(f"{API_BASE_URL}get_progress")
    progress_response.raise_for_status()
    last_completed_batch = progress_response.json().get('last_completed_batch', -1)
    # Der nächste zu verarbeitende Batch ist der darauffolgende
    current_batch_index = last_completed_batch + 1
    print(f"Server meldet: Setze Verarbeitung bei Batch #{current_batch_index} fort.")
except Exception as e:
    print(f"Konnte Fortschritt nicht vom Server laden ({e}). Starte bei Batch #0.")
    current_batch_index = 0

while True:
    print(f"Frage Batch #{current_batch_index} vom Daten-Server an...")
    
    try:
        # 1. Daten vom Server holen
        response = requests.get(f"{API_BASE_URL}get_batch?batch={current_batch_index}", timeout=60)
        response.raise_for_status() # Fehler bei HTTP-Status > 400
        data = response.json()

        # 2. Prüfen, ob das Ende erreicht ist
        if data.get("status") == "eof":
            print("Ende des Datensatzes erreicht. Prozess erfolgreich beendet.")
            break
        
        if data.get("status") != "ok":
            print(f"Fehler vom Server erhalten: {data.get('message')}")
            break

        raw_lines = data.get("lines", [])
        if not raw_lines:
            print("Leerer Batch empfangen, obwohl nicht EOF. Breche ab.")
            break

        # 3. Den empfangenen Batch verarbeiten
        texts_to_embed = []
        payloads = []
        
        # Schleife durch die rohen JSON-Strings im Batch
        for line in raw_lines:
            try:
                paper = json.loads(line)
                
                title = paper.get('title', '')
                abstract = paper.get('abstract', '')
                
                if not abstract or not title: continue

                full_text_for_embedding = f"{sanitize_text(title)}. {sanitize_text(abstract)}"
                texts_to_embed.append(full_text_for_embedding)
                
                payloads.append({"type": "arXiv", "original_json": paper})

            except json.JSONDecodeError:
                print(f"Warnung: Überspringe fehlerhafte JSON-Zeile.")
                continue
        
        # 4. Kodieren und Hochladen, wenn es etwas zu verarbeiten gibt
        if texts_to_embed:
            print(f"Verarbeite {len(texts_to_embed)} Einträge aus Batch #{current_batch_index}...")
            
            # Vektoren auf der GPU erstellen
            vectors = encode_documents_for_db_batch(model, texts_to_embed, batch_size=PROCESSING_BATCH_SIZE)
            
            # In die Datenbank schreiben
            upsert_points_to_db(db_client, COLLECTION_NAME, vectors, payloads)
        
        # 5. Zum nächsten Batch übergehen
        requests.post(f"{API_BASE_URL}/update_progress", json={"last_completed_batch": current_batch_index})
        current_batch_index += 1

        # 6. Fortschritt speichern
        with open("last_batch.txt", "w") as f:
            f.write(str(current_batch_index))
            
    except requests.exceptions.RequestException as e:
        print(f"Netzwerkfehler: {e}. Warte 60 Sekunden und versuche es erneut...")
        time.sleep(60)
    except Exception as e:
        print(f"Ein unerwarteter Fehler ist aufgetreten: {e}. Breche ab.")
        # Optional: Hier könnten Sie den aktuellen Batch-Index ausgeben, um den Neustart zu erleichtern
        print(f"Fehler trat bei der Verarbeitung von Batch #{current_batch_index} auf.")
        break