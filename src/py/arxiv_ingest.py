import json
import re

import vectorizer
import database_manager as db

def sanitize_text(text: str) -> str:
    """
    Bereinigt arXiv-Abstracts.
    Ersetzt Zeilenumbrüche und überflüssige Leerzeichen.
    Versucht, LaTeX-ähnliche mathematische Formatierungen zu entfernen.
    """
    if not text:
        return ""
    
    # 0. Leerzeichen vor und nach dem Text werden entfernt
    text = text.strip()

    # 1. Ersetze alle Newline-Zeichen und Tabulatoren durch ein Leerzeichen
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    
    # 2. Entferne LaTeX-Artefakte
    text = text.replace('/', '').replace('\\', '').replace('$', '').replace('{', ' ').replace('}', ' ').replace('_', '')

    # 3. Ersetze mehrere Leerzeichen durch ein einziges
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def process_arxiv_dataset(filepath: str, batch_size: int = 128):
    """
    Liest den arXiv JSON-Datensatz Zeile für Zeile, bereinigt ihn
    und fügt ihn in Batches in die Qdrant-Datenbank ein.
    """
    print(f"Starte Verarbeitung des arXiv-Datensatzes: {filepath}")
    
    db.ensure_collection_exists()

    texts_to_embed = []
    payloads = []

    # Die Datei wird Zeile für Zeile gelesen
    with open(filepath, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            try:
                # Jede Zeile ist ein separates JSON-Objekt
                paper = json.loads(line)
                
                title = paper.get('title', '')
                abstract = paper.get('abstract', '')
                
                # Filter: Überspringe Einträge ohne Abstract oder Titel
                if not abstract or not title:
                    continue

                snitized_title = sanitize_text(title)
                snitized_abstract = sanitize_text(abstract)
                full_text_for_embedding = f"{snitized_title}. {snitized_abstract}"
                
                texts_to_embed.append(full_text_for_embedding)
                
                # Erstellen des Metadaten-Payloads
                payloads.append({
                    "type": "arXiv",
                    "original_json": paper
                })

                # Der volle batch wird verarbeitet
                if len(texts_to_embed) >= batch_size:
                    print(f"Verarbeite Batch #{i // batch_size + 1}...")
                    
                    # Vektoren erstellen
                    vectors = vectorizer.encode_batch(texts_to_embed)
                    
                    # In die Datenbank schreiben
                    db.upsert_points(vectors, payloads)
                    
                    # Listen für den nächsten Batch leeren
                    texts_to_embed = []
                    payloads = []

            except json.JSONDecodeError:
                # Falls eine Zeile mal fehlerhaftes JSON enthält
                print(f"Warnung: Überspringe fehlerhafte JSON-Zeile #{i+1}")
                continue

    # Der letzte, unvollständige Batch
    if texts_to_embed:
        print("Verarbeite den letzten Batch...")
        vectors = vectorizer.encode_batch(texts_to_embed)
        db.upsert_points(vectors, payloads)

    print("Verarbeitung des arXiv-Datensatzes abgeschlossen.")


if __name__ == "__main__":
    ARXIV_FILE_PATH = "./arxiv-metadata-oai-snapshot.json" 
    process_arxiv_dataset(ARXIV_FILE_PATH)