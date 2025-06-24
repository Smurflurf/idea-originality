import sys
import socket
from flask import Flask, request, jsonify
from sentence_transformers import SentenceTransformer, models as models

def setup_vectorizer():
    MODEL_NAME = 'dwzhu/e5-base-4k'
    print(f"Loading sentence-transformer-model: '{MODEL_NAME}'...")
    word_embedding_model = models.Transformer(MODEL_NAME)
    pooling_model = models.Pooling(word_embedding_model.get_word_embedding_dimension())
    try:
        model = SentenceTransformer(modules=[word_embedding_model, pooling_model], device='cuda')
    except: 
        model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
    return model

app = Flask(__name__)

@app.route("/ping", methods=["GET"])
def ping_endpoint():
    """
    Endpoint to check if the server is alive and running.
    """
    return "pong", 200

@app.route("/vectorize", methods=["POST"])
def vectorize_endpoint():
    """
    Takes raw text from the queries body, vectorizes it and returns the vector as a JSON.
    """
    model = setup_vectorizer()
    try:
        text_to_vectorize = request.data.decode('utf-8')
        
        if not text_to_vectorize:
            return jsonify({"error": "Request body must contain text"}), 400

        embedding = model.encode(text_to_vectorize)
        vector = embedding.tolist()
        
        return jsonify({"vector": vector})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    if len(sys.argv) > 1:
        try:
            callback_port = int(sys.argv[1])
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('127.0.0.1', callback_port))
                s.sendall(b'ready')
            print(f"Python-Server: 'Ready'-signal sent to Port {callback_port}.")
        except Exception as e:
            print(f"Python-Server: Error sending the 'Ready'-signal: {e}", file=sys.stderr)
    app.run(host='127.0.0.1', port=5001, debug=False)