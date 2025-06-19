from flask import Flask, jsonify, request
import os
import math
import threading
import random

ARXIV_FILE_PATH = "./arxiv-metadata-oai-snapshot.json"
BATCH_LIST_FILE = "./arxiv_batches.txt" 
INDEX_FILE = "./arxiv_index.txt"
BATCH_SIZE = 32

lock = threading.Lock()
app = Flask(__name__)

def initialize_files():
    """
    Erstellt die Batch-Datei UND die Index-Datei, falls sie nicht existieren.
    Diese Funktion ist jetzt die aufwendigste, wird aber nur einmal ausgeführt.
    """
    if os.path.exists(BATCH_LIST_FILE) and os.path.exists(INDEX_FILE):
        print("Batch- und Index-Dateien existieren bereits. Initialisierung übersprungen.")
        return

    print("Initialisiere Batch- und Index-Dateien...")
    try:
        line_offsets = [0] # Der Index der Byte-Positionen, Zeile 0 startet bei Byte 0
        line_count = 0
        
        with open(ARXIV_FILE_PATH, 'rb') as f: # im Binär-Modus öffnen für seek/tell
            while f.readline():
                line_offsets.append(f.tell())
                line_count += 1
        
        # Der letzte Eintrag ist die Dateigröße
        line_offsets.pop()

        # Speichere den Index
        with open(INDEX_FILE, 'w') as f:
            for offset in line_offsets:
                f.write(f"{offset}\n")
        print(f"Index-Datei '{INDEX_FILE}' mit {len(line_offsets)} Einträgen erfolgreich erstellt.")

        # Speichere die Batch-Liste
        total_batches = math.ceil(line_count / BATCH_SIZE)
        all_batch_indices = list(range(total_batches))
        with open(BATCH_LIST_FILE, 'w') as f:
            f.write(",".join(map(str, all_batch_indices)))
        print(f"Batch-Datei '{BATCH_LIST_FILE}' mit {total_batches} Batches erfolgreich erstellt.")

    except Exception as e:
        print(f"FEHLER bei der Initialisierung: {e}")

# lade den kompletten Index beim Start in den Speicher
try:
    with open(INDEX_FILE, 'r') as f:
        LINE_OFFSETS = [int(line) for line in f]
    print(f"Datei-Index mit {len(LINE_OFFSETS)} Positionen in den Speicher geladen.")
except FileNotFoundError:
    LINE_OFFSETS = []
    print("WARNUNG: Index-Datei nicht gefunden. Der Server wird langsam sein.")


@app.route('/get_random_batch', methods=['GET'])
def get_batch():
    """
    Wählt einen zufälligen, noch nicht erledigten Batch-Index aus der Liste aus
    und sendet die zugehörigen Daten an den Client.
    """
    with lock:
        try:
            with open(BATCH_LIST_FILE, 'r') as f:
                content = f.read().strip()
                if not content:
                    return jsonify({"status": "eof", "message": "Alle Batches wurden verarbeitet."})
                
                remaining_indices = [int(i) for i in content.split(',') if i]

            if not remaining_indices:
                return jsonify({"status": "eof", "message": "Alle Batches wurden verarbeitet."})

            batch_to_process = random.choice(remaining_indices)
            num_batches_left = len(remaining_indices)

        except (FileNotFoundError, IndexError):
            return jsonify({"status": "error", "message": f"Batch-Datei '{BATCH_LIST_FILE}' nicht gefunden oder leer."}), 500
    try:
        start_line = batch_to_process * BATCH_SIZE
        
        if start_line >= len(LINE_OFFSETS):
            return jsonify({"status": "eof", "message": "Batch-Index außerhalb des Bereichs."})

        # Holen der exakten Start-Byte-Position aus unserem geladenen Index
        start_offset = LINE_OFFSETS[start_line]
        
        batch_lines = []
        with open(ARXIV_FILE_PATH, 'r', encoding='utf-8') as f:
            # Springe sofort zur richtigen Stelle in der Datei
            f.seek(start_offset)
            # Lese nur die 32 Zeilen, die wir brauchen
            for _ in range(BATCH_SIZE):
                line = f.readline()
                if not line: break
                batch_lines.append(line)
        
        print(f"Vergebe zufälligen Task: Batch #{batch_to_process}.")
        return jsonify({
            "status": "ok", 
            "lines": batch_lines, 
            "batch_index": batch_to_process,
            "batches_left": num_batches_left
        })
    except Exception as e:
        return jsonify({"status": "error", "message": f"Fehler beim Lesen der Daten für Batch #{batch_to_process}: {e}"}), 500


@app.route('/complete_batch', methods=['POST'])
def complete_batch():
    """
    Entfernt einen spezifischen, erfolgreich verarbeiteten Batch-Index
    aus der 'arxiv_batches.txt'-Datei auf thread-sichere Weise.
    """
    data = request.get_json()
    batch_to_remove = data.get('batch_index')

    if batch_to_remove is None:
        return jsonify({"status": "error", "message": "Parameter 'batch_index' fehlt."}), 400

    with lock:
        try:
            with open(BATCH_LIST_FILE, 'r') as f:
                content = f.read().strip()
                remaining_indices = set(int(i) for i in content.split(',') if i)

            if int(batch_to_remove) in remaining_indices:
                remaining_indices.remove(int(batch_to_remove))
                
                sorted_indices = sorted(list(remaining_indices))
                with open(BATCH_LIST_FILE, 'w') as f:
                    f.write(",".join(map(str, sorted_indices)))
                
                message = f"Batch #{batch_to_remove} erfolgreich entfernt. Verbleibende Tasks: {len(sorted_indices)}"
                print(message)
                return jsonify({"status": "ok", "message": message})
            else:
                message = f"Batch #{batch_to_remove} war bereits entfernt. Ignoriere."
                print(message)
                return jsonify({"status": "already_removed", "message": message})

        except FileNotFoundError:
            return jsonify({"status": "error", "message": f"Batch-Datei '{BATCH_LIST_FILE}' nicht gefunden."}), 500
        except Exception as e:
            return jsonify({"status": "error", "message": f"Fehler beim Entfernen des Batches: {e}"}), 500


if __name__ == '__main__':
    initialize_files()
    app.run(host='0.0.0.0', port=8000)