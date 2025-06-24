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
    Creates the batches and index file, if they don't exist.
    Usually happens only once.
    """
    if os.path.exists(BATCH_LIST_FILE) and os.path.exists(INDEX_FILE):
        print("Batch- and index-files already exist. No initialisation needed.")
        return

    print("Initialising batch- and index-files...")
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
        print(f"Index-file '{INDEX_FILE}' with {len(line_offsets)} entries successfully created.")

        # Speichere die Batch-Liste
        total_batches = math.ceil(line_count / BATCH_SIZE)
        all_batch_indices = list(range(total_batches))
        with open(BATCH_LIST_FILE, 'w') as f:
            f.write(",".join(map(str, all_batch_indices)))
        print(f"Batch-file '{BATCH_LIST_FILE}' with {total_batches} Batches successfully created.")

    except Exception as e:
        print(f"Error initialising: {e}")

# lade den kompletten Index beim Start in den Speicher
try:
    with open(INDEX_FILE, 'r') as f:
        LINE_OFFSETS = [int(line) for line in f]
    print(f"File-index with {len(LINE_OFFSETS)} positions loaded into memory.")
except FileNotFoundError:
    LINE_OFFSETS = []
    print("WARNING: did not find index-file. Server will be slow.")


@app.route('/get_random_batch', methods=['GET'])
def get_batch():
    """
    Chooses a random, not completed Batch-index from the list
    and sends it with the associated data to the client.
    """
    with lock:
        try:
            with open(BATCH_LIST_FILE, 'r') as f:
                content = f.read().strip()
                if not content:
                    return jsonify({"status": "eof", "message": "All batches were processed."})
                
                remaining_indices = [int(i) for i in content.split(',') if i]

            if not remaining_indices:
                return jsonify({"status": "eof", "message": "All batches were processed."})

            batch_to_process = random.choice(remaining_indices)
            num_batches_left = len(remaining_indices)

        except (FileNotFoundError, IndexError):
            return jsonify({"status": "error", "message": f"Batch-file '{BATCH_LIST_FILE}' could not be found or is empty."}), 500
    try:
        start_line = batch_to_process * BATCH_SIZE
        
        if start_line >= len(LINE_OFFSETS):
            return jsonify({"status": "eof", "message": "Batch-index out of bounds."})

        # Holen der exakten Start-Byte-Position aus dem geladenen Index
        start_offset = LINE_OFFSETS[start_line]
        
        batch_lines = []
        with open(ARXIV_FILE_PATH, 'r', encoding='utf-8') as f:
            # Springe zur richtigen Stelle in der Datei
            f.seek(start_offset)
            # Lese nur die benötigten 32 Zeilen
            for _ in range(BATCH_SIZE):
                line = f.readline()
                if not line: break
                batch_lines.append(line)
        
        print(f"Assigning random Task: Batch #{batch_to_process}.")
        return jsonify({
            "status": "ok", 
            "lines": batch_lines, 
            "batch_index": batch_to_process,
            "batches_left": num_batches_left
        })
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error reading data for Batch #{batch_to_process}: {e}"}), 500


@app.route('/complete_batch', methods=['POST'])
def complete_batch():
    """
    Removes a specific, successfully processed Batch-index from the 'arxiv_batches.txt'-file; Thread-safe.
    """
    data = request.get_json()
    batch_to_remove = data.get('batch_index')

    if batch_to_remove is None:
        return jsonify({"status": "error", "message": "Parameter 'batch_index' missing."}), 400

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
                
                message = f"Batch #{batch_to_remove} removed successfully. Remaining Tasks: {len(sorted_indices)}"
                print(message)
                return jsonify({"status": "ok", "message": message})
            else:
                message = f"Batch #{batch_to_remove} was already removed. Ignore."
                print(message)
                return jsonify({"status": "already_removed", "message": message})

        except FileNotFoundError:
            return jsonify({"status": "error", "message": f"Batch-file '{BATCH_LIST_FILE}' not found."}), 500
        except Exception as e:
            return jsonify({"status": "error", "message": f"Error removing the Batch: {e}"}), 500


if __name__ == '__main__':
    initialize_files()
    app.run(host='0.0.0.0', port=8000, threaded=True)