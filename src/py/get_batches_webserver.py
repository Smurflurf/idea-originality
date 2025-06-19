from flask import Flask, jsonify, request
import os

ARXIV_FILE_PATH = "./arxiv-metadata-oai-snapshot.json"
PROGRESS_FILE = "./progress.txt" 
BATCH_SIZE = 32

app = Flask(__name__)

def get_file_handle_at_line(line):
    """Öffnet die Datei und gibt ein Handle zurück, das an einer bestimmten Zeile beginnt."""
    f = open(ARXIV_FILE_PATH, 'r', encoding='utf-8')
    if line > 0:
        print(f"Springe zu Zeile {line}...")
        for _ in range(line):
            if not f.readline(): # Falls die Zeilennummer außerhalb des Bereichs liegt
                break
    return f

@app.route('/get_progress', methods=['GET'])
def get_progress():
    """
    Liest den zuletzt erfolgreich verarbeiteten Batch-Index aus der Speicherdatei.
    """
    try:
        with open(PROGRESS_FILE, 'r') as f:
            last_batch = int(f.read().strip())
            print(f"Letzter bekannter Fortschritt ist Batch #{last_batch}.")
            return jsonify({"status": "ok", "last_completed_batch": last_batch})
    except (FileNotFoundError, ValueError):
        print("Keine Fortschrittsdatei gefunden, starte bei -1.")
        return jsonify({"status": "ok", "last_completed_batch": -1})

@app.route('/update_progress', methods=['POST'])
def update_progress():
    """
    Empfängt einen neuen Fortschritt vom Client (Colab) und speichert ihn.
    """
    data = request.get_json()
    if not data or 'last_completed_batch' not in data:
        return jsonify({"status": "error", "message": "Parameter 'last_completed_batch' fehlt"}), 400
        
    try:
        last_batch = int(data['last_completed_batch'])
        with open(PROGRESS_FILE, "w") as f:
            f.write(str(last_batch))
        print(f"Fortschritt erfolgreich auf Batch #{last_batch} gesetzt.")
        return jsonify({"status": "ok", "message": f"Fortschritt auf Batch #{last_batch} gesetzt."})
    except (ValueError, TypeError):
        return jsonify({"status": "error", "message": "Ungültiger Wert für 'last_completed_batch'"}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": f"Fehler beim Speichern des Fortschritts: {e}"}), 500

@app.route('/get_batch', methods=['GET'])
def get_batch():
    try:
        batch_index_str = request.args.get('batch', default=None, type=str)
        
        if batch_index_str is None:
            return jsonify({"status": "error", "message": "Parameter 'batch' fehlt."}), 400

        batch_index = int(batch_index_str)
        start_line = batch_index * BATCH_SIZE

        file_handle = get_file_handle_at_line(start_line)
        
        batch_lines = []
        for _ in range(BATCH_SIZE):
            line = file_handle.readline()
            if not line: 
                break
            batch_lines.append(line)
        
        file_handle.close() 

        print(f"Sende Batch #{batch_index} (Zeilen {start_line} bis {start_line + len(batch_lines) - 1})")
        
        if not batch_lines:
            return jsonify({"status": "eof", "message": "Ende des Datensatzes erreicht oder Batch-Index zu hoch."})
            
        return jsonify({"status": "ok", "lines": batch_lines, "next_batch_index": batch_index + 1})

    except ValueError:
        return jsonify({"status": "error", "message": "Ungültiger 'batch'-Parameter. Muss eine Zahl sein."}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": f"Ein interner Fehler ist aufgetreten: {e}"}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)