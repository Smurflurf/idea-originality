import vectorizer
import database_manager as db

def main_test():
    print("\n--- Starte Test ---")
    db.ensure_collection_exists()
    print("Idee eingeben: ")
    sentence = input()
    vector = vectorizer.encode_text(sentence)
    
    print(f"Suche nach den 3 nächsten Nachbarn für den Test-Satz '{sentence}' in '{db.COLLECTION_NAME}'...")
    search_results = db.client.search(
        collection_name=db.COLLECTION_NAME,
        query_vector=vector,
        limit=3,  # Top 3 Ergebnisse
        with_payload=True
    )

    print("\n--- Suchergebnisse ---")
    if not search_results:
        print("Keine Ergebnisse gefunden. Ist die Datenbank leer?")
    else:
        for i, hit in enumerate(search_results):
            print(f"\n--- Treffer #{i+1} ---")
            print(f"Ähnlichkeits-Score: {hit.score:.4f}")
            
            payload = hit.payload 
            
            source = payload.get("type", "Unbekannt")
            original_data = payload.get("original_json", {})
            title = original_data.get("title", "").replace('\n', '')
            abstract_preview = original_data.get("abstract", "")[:150] + "..."

            print(f"Quelle: {source}")
            print(f"Titel: {title}")
            print(f"Abstract-Vorschau: {abstract_preview}")

if __name__ == "__main__":
    main_test()