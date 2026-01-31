import numpy as np
import json
import time
import sys
import socket
import os
import gc 
import joblib
import hdbscan
import contextlib 
import fasttext
import torch
import pycountry
import wave
import io
import re
import struct
import unicodedata
from piper import PiperVoice
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, TextIteratorStreamer
from huggingface_hub import hf_hub_download, HfApi
from threading import Thread
from flask import Flask, request, jsonify, Response, stream_with_context
from sentence_transformers import SentenceTransformer, models as st_models
from sklearn.preprocessing import Normalizer

# --- Globale Variablen ---
vectorizer_model = None
translation_resources = {} # Cache für Übersetzungs- und Spracherkennungsmodelle
TRANSLATION_TOKEN_LIMIT = 512
cached_languages_list = []
umap_models = {}
hdbscan_model_paths = {} 
dir = os.path.dirname(os.path.abspath(__file__))

# --- Piper TTS Konfiguration ---
PIPER_MODEL_CACHE = {} # Cache für geladene PiperVoice Objekte
PIPER_REPO_ID = "rhasspy/piper-voices"
PIPER_MODELS_DIR = os.path.join(dir, "piper_models") 
_HF_REPO_FILES_CACHE = None

def setup_vectorizer():
    """
    Initialisiert das SentenceTransformer-Modell.
    """
    global vectorizer_model
    if vectorizer_model is None:
        MODEL_NAME = 'Qwen/Qwen3-Embedding-0.6B'
        print(f"Loading sentence-transformer-model: '{MODEL_NAME}'...")
        try:
            vectorizer_model = SentenceTransformer(MODEL_NAME, device='cuda', trust_remote_code=True, local_files_only=True)
            print("Vectorizer model loaded on CUDA.")
        except Exception as e:
            print(f"CUDA not available or failed, falling back to CPU. Error: {e}")
            vectorizer_model = SentenceTransformer(MODEL_NAME, trust_remote_code=True, local_files_only=True)
            print("Vectorizer model loaded on CPU.")
    return vectorizer_model

def setup_translator():
    """
    Initialisiert die Modelle für Übersetzung (NLLB) und Spracherkennung (GlotLID).
    Original-Version: Lädt das Modell im Standard-Format (float32) ohne Quantisierung.
    """
    global translation_resources
    if not translation_resources:
        print("Loading translation and language detection models...")
        
        # 1. GlotLID laden
        translation_resources['lid'] = fasttext.load_model(hf_hub_download("cis-lmu/glotlid", "model_v3.bin"))

        # 2. NLLB laden        
        #model_id = "facebook/nllb-200-3.3B"
        #model_id = "facebook/nllb-200-distilled-1.3B"
        model_id = "facebook/nllb-200-distilled-600M"
        translation_resources['tok'] = AutoTokenizer.from_pretrained(model_id)
        
        print(f"Lade NLLB Modell ({model_id}) im Standard-Modus...")
        translation_resources['mod'] = AutoModelForSeq2SeqLM.from_pretrained(model_id)
        
        # Config setzen
        translation_resources['tok'].model_max_length = TRANSLATION_TOKEN_LIMIT
        translation_resources['codes'] = set(translation_resources['tok'].additional_special_tokens)
        print("Translation resources successfully loaded.")

def _resolve_nllb_code(iso_code, supported_codes):
    """
    Wandelt ISO-Codes (de, deu) in NLLB-Codes (deu_Latn) um.
    """
    try:
        # 1. Versuche ISO Alpha-2 (z.B. "de")
        lang = pycountry.languages.get(alpha_2=iso_code)
        if not lang:
            # 2. Versuche ISO Alpha-3 (z.B. "deu")
            lang = pycountry.languages.get(alpha_3=iso_code)
        
        if lang:
            iso3 = lang.alpha_3
            # Suche in den NLLB Codes nach einem, der mit 'deu_' beginnt
            match = next((code for code in supported_codes if code.startswith(iso3 + "_")), None)
            return match
    except:
        pass
    return None

def load_umap_models():
    """Lädt die UMAP-Modelle."""
    global umap_models
    models_to_load = {"global": "global_umap_model_10_DIM.pkl", "local": "local_umap_model_10_DIM.pkl", "2d": "umap_model_2_DIM.pkl"}
    print(f"Searching for UMAP models in directory: {dir}")
    for model_type, model_file in models_to_load.items():
        model_path = os.path.join(dir, model_file)
        if os.path.exists(model_path):
            print(f"Loading UMAP model for type '{model_type}' from {model_path}...")
            try:
                model_instance = joblib.load(model_path, mmap_mode='r+')
                if hasattr(model_instance, 'verbose'): model_instance.verbose = False
                umap_models[model_type] = model_instance
                print(f"Successfully loaded model for type '{model_type}'.")
            except Exception as e:
                print(f"Error loading UMAP model from {model_path}: {e}", file=sys.stderr)
        else:
            print(f"UMAP model file not found: {model_path}", file=sys.stderr)
    if "2d" in umap_models: umap_models[2] = umap_models["2d"]
    if "global" in umap_models: umap_models[10] = umap_models["local"]

def load_hdbscan_models():
    """Entdeckt die HDBSCAN-Modellpfade."""
    global hdbscan_model_paths
    model_base_path = os.path.join(dir, "hdbscan_models")
    print(f"Discovering HDBSCAN models in directory: {model_base_path}")
    if not os.path.isdir(model_base_path):
        print(f"HDBSCAN model directory not found, skipping: {model_base_path}")
        return
    count = 0
    for filename in os.listdir(model_base_path):
        if filename.startswith("hdbscan_") and filename.endswith(".pkl"):
            cluster_id = filename[len("hdbscan_"):-len(".pkl")]
            hdbscan_model_paths[cluster_id] = os.path.join(model_base_path, filename)
            count += 1
    print(f"-> Discovered {count} HDBSCAN model paths.")

# Hilfsfunktion, um die Sprachen einmalig zu generieren
def _generate_language_list():
    global cached_languages_list
    if cached_languages_list:
        return cached_languages_list
    
    #setup_translator()
    unique_langs = {}
    # NLLB Codes sind z.B. 'deu_Latn', 'eng_Latn'
    for code in translation_resources['codes']:
        try:
            # Wir trennen 'deu' von '_Latn'
            iso3 = code.split('_')[0]
            # Wir wollen keine Duplikate (z.B. srp_Cyrl und srp_Latn -> nur einmal Serbian)
            if iso3 in unique_langs:
                continue

            lang_obj = pycountry.languages.get(alpha_3=iso3)
            if lang_obj:
                # Wir bevorzugen den 2-Letter Code (de) für das Frontend, falls vorhanden
                frontend_code = getattr(lang_obj, 'alpha_2', iso3)
                name = lang_obj.name.split(';')[0] # Manchmal gibt es mehrere Namen, wir nehmen den ersten
                unique_langs[iso3] = {
                    "code": frontend_code,
                    "name": name
                }
        except:
            continue
    # In eine Liste umwandeln und alphabetisch nach Namen sortieren
    cached_languages_list = sorted(unique_langs.values(), key=lambda x: x['name'])
    return cached_languages_list

def chunk_text(text, tokenizer, max_tokens=1000):
    """Teilt Text intelligent in Stücke, die das Token-Limit nicht sprengen."""
    tokens = tokenizer(text, return_tensors="pt", add_special_tokens=False).input_ids[0]
    if len(tokens) <= max_tokens:
        yield text
        return

    # Fallback: Einfaches Splitten nach Sätzen (bei '.' oder '\n')
    # Für eine perfekte Lösung bräuchte man nltk sent_tokenize, aber das hier reicht meistens
    sentences = text.replace('\n', ' \n ').split('. ')
    current_chunk = []
    current_len = 0
    
    for sentence in sentences:
        # Grobe Schätzung: 1 Wort ~= 1.3 Tokens (schneller als jedes Mal den Tokenizer zu rufen)
        sent_len = len(sentence.split()) * 1.5 
        
        if current_len + sent_len > max_tokens:
            yield ". ".join(current_chunk) + "."
            current_chunk = [sentence]
            current_len = sent_len
        else:
            current_chunk.append(sentence)
            current_len += sent_len
            
    if current_chunk:
        yield ". ".join(current_chunk)

def sanitize_for_tts(text):
    """
    Reinigt den Text von unsichtbaren Steuerzeichen, die Piper/espeak-ng crashen lassen.
    """
    if not text:
        return ""
    
    # 1. Unicode Normalisierung (NFKC löst Ligaturen auf und standardisiert Zeichen)
    text = unicodedata.normalize('NFKC', text)
    
    # 2. Explizites Ersetzen von bekannten Problem-Zeichen
    # \xa0 = Non-breaking space (häufigste Crash-Ursache aus HTML)
    # \u200b = Zero-width space
    # \xad = Soft hyphen
    text = text.replace('\xa0', ' ').replace('\u200b', '').replace('\xad', '')
    
    # 3. Alle Whitespaces (Tabs, Newlines) zu einzelnen Leerzeichen zusammenfassen
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def smart_split_tts(text, max_chars=500):
    """
    Zerlegt Text in Sätze/Abschnitte, damit das TTS-Modell nicht OOM geht.
    """
    # 1. Grob nach Satzzeichen splitten, dabei Satzzeichen behalten
    # Regex: Splitte bei . ! ? aber behalte das Zeichen im Ergebnis
    parts = re.split(r'([.!?]+)', text)
    
    current_chunk = ""
    
    for part in parts:
        # Wenn der Chunk zu lang wird, yielden
        if len(current_chunk) + len(part) > max_chars:
            if current_chunk.strip():
                yield current_chunk.strip()
            current_chunk = part
        else:
            current_chunk += part
            
    # Rest ausgeben
    if current_chunk.strip():
        yield current_chunk.strip()

def _get_best_voice_path(lang_code):
    """
    Sucht dynamisch nach dem besten Modell für eine Sprache im HuggingFace Repo.
    Priorität: High > Medium > Low.
    Fallback: Englisch, falls Sprache nicht gefunden.
    """
    global _HF_REPO_FILES_CACHE
    
    # 1. Dateiliste holen (nur einmalig)
    if _HF_REPO_FILES_CACHE is None:
        print("TTS: Fetching file list from HuggingFace repository...")
        try:
            api = HfApi()
            _HF_REPO_FILES_CACHE = api.list_repo_files(PIPER_REPO_ID)
        except Exception as e:
            print(f"TTS: Error fetching repo files: {e}")
            return None

    # 2. Kandidaten filtern
    # Piper Repo Struktur ist meist: lang_code/country_code/voice_name/quality/file.onnx
    # Wir suchen nach Dateien, die mit lang_code/ beginnen und auf .onnx enden.
    candidates = [f for f in _HF_REPO_FILES_CACHE if f.startswith(f"{lang_code}/") and f.endswith(".onnx")]

    # 3. Fallback auf Englisch, wenn nichts gefunden
    if not candidates:
        if lang_code != 'en':
            print(f"TTS: No voices found for '{lang_code}', falling back to 'en'.")
            return _get_best_voice_path('en')
        else:
            print("TTS: Critical Error - No voices found even for English.")
            return None

    # 4. Sortieren nach Qualität
    # Wir vergeben Punkte: high=3, medium=2, low=1, rest=0
    def quality_score(filename):
        if "high" in filename: return 3
        if "medium" in filename: return 2
        if "low" in filename: return 1
        return 0
    
    # Sortieren: Erst nach Score absteigend, dann alphabetisch (für Determinismus)
    candidates.sort(key=lambda x: (quality_score(x), x), reverse=True)
    
    # Der erste Eintrag ist der "Beste"
    best_match = candidates[0]
    print(f"TTS: Selected model '{best_match}' for language '{lang_code}'")
    return best_match

def get_or_load_piper_model(lang_code):
    """
    Lädt das Piper-Modell. Nutzt dynamische Pfadermittlung.
    Verhindert Duplikate bei Fallbacks (z.B. wird 'xy' -> 'en' im 'en'-Ordner gespeichert).
    """
    global PIPER_MODEL_CACHE
    
    # 1. Cache prüfen (Hat 'xy' schon einen Eintrag?)
    if lang_code in PIPER_MODEL_CACHE:
        return PIPER_MODEL_CACHE[lang_code]

    # 2. Den besten Pfad im Repo finden
    hf_relative_path = _get_best_voice_path(lang_code)
    if not hf_relative_path:
        raise Exception(f"Could not find any Piper model for language '{lang_code}'")

    # 3. Tatsächliche Sprache aus dem Pfad ableiten
    # Der Pfad sieht so aus: "en/en_US/lessac/high/en_US-lessac-high.onnx"
    # Wir nehmen den ersten Teil ("en") als den echten Sprachcode.
    resolved_lang = hf_relative_path.split('/')[0]

    # 4. Cache prüfen (Hat die aufgelöste Sprache 'en' schon einen Eintrag?)
    # Das spart RAM: 'xy' und 'en' zeigen dann auf dasselbe Objekt.
    if resolved_lang in PIPER_MODEL_CACHE:
        print(f"TTS: Using cached model for '{resolved_lang}' (requested '{lang_code}').")
        voice = PIPER_MODEL_CACHE[resolved_lang]
        PIPER_MODEL_CACHE[lang_code] = voice # Alias setzen für zukünftige Anfragen
        return voice

    # Dateinamen vorbereiten
    filename = os.path.basename(hf_relative_path)
    config_filename = filename + ".json" 
    hf_folder_path = os.path.dirname(hf_relative_path)

    # 5. Herunterladen
    # WICHTIG: Wir nutzen 'resolved_lang' für den lokalen Ordner, nicht 'lang_code'!
    # So landet das englische Modell immer in piper_models/en/, egal was angefragt wurde.
    target_dir = os.path.join(PIPER_MODELS_DIR, resolved_lang)
    
    print(f"TTS: Ensuring model '{filename}' is available in '{target_dir}'...")
    
    # Modell (.onnx)
    model_path = hf_hub_download(
        repo_id=PIPER_REPO_ID,
        filename=hf_relative_path, 
        local_dir=target_dir,
        local_dir_use_symlinks=False
    )
    
    # Config (.onnx.json)
    config_path = hf_hub_download(
        repo_id=PIPER_REPO_ID,
        filename=os.path.join(hf_folder_path, config_filename),
        local_dir=target_dir,
        local_dir_use_symlinks=False
    )

    print(f"TTS: Loading Piper model from {model_path}...")
    
    # 6. Piper Voice laden
    voice = PiperVoice.load(model_path, config_path=config_path, use_cuda=False)
    
    # 7. Caching
    # Wir speichern es unter der echten Sprache UND der angefragten Sprache
    PIPER_MODEL_CACHE[resolved_lang] = voice
    if lang_code != resolved_lang:
        PIPER_MODEL_CACHE[lang_code] = voice
        
    return voice

    
    
def predict_language(text, output_format='iso', max_length=2000):
    """
    Zentrale Spracherkennung.
    Passt GlotLID (ISO-3) auf Piper Folder-Namen (ISO-2) an.
    """
    if 'lid' not in translation_resources:
        setup_translator()
        
    try:
        clean_text = text.replace("\n", " ")[:max_length]
        prediction = translation_resources['lid'].predict(clean_text)
        label = prediction[0][0].replace("__label__", "")
        
        # --- Modus 1: NLLB (Übersetzung) ---
        if output_format == 'nllb':
            if label in translation_resources['codes']:
                return label
            
            iso3 = label.split('_')[0]
            candidates = [iso3]
            try:
                l = pycountry.languages.get(alpha_3=iso3)
                if l and hasattr(l, 'macro_language'): 
                    candidates.append(l.macro_language.alpha_3)
            except: pass
            
            match = next((x for c in candidates for x in translation_resources['codes'] if x.startswith(c+"_")), None)
            return match if match else "eng_Latn"

        # --- Modus 2: ISO (TTS/Piper) ---
        else:
            iso3_code = label.split('_')[0]
            
            # --- MANUELLE OVERRIDES FÜR PIPER FOLDER ---
            
            # 1. Chinesisch: cmn (Mandarin) -> zh
            if iso3_code in ['cmn', 'zho']:
                return 'zh'
            
            # 2. Norwegisch: nob (Bokmål), nno (Nynorsk) -> no
            # Pycountry würde 'nob' zu 'nb' machen, aber Piper hat nur 'no'!
            if iso3_code in ['nob', 'nno', 'nor']:
                return 'no'
            
            # 3. Serbisch/Kroatisch/Bosnisch
            # Piper hat nur 'sr' (Serbisch). Da die Sprachen sehr ähnlich sind,
            # KÖNNTE man hr/bs auf sr mappen. Hier nur exaktes Serbisch:
            if iso3_code == 'srp':
                return 'sr'
            
            # --- Standard ISO-2 Auflösung via pycountry ---
            # Das deckt ab: deu->de, fra->fr, spa->es, ukr->uk, pol->pl, etc.
            try:
                lang_obj = pycountry.languages.get(alpha_3=iso3_code)
                if lang_obj and hasattr(lang_obj, 'alpha_2'):
                    return lang_obj.alpha_2
            except:
                pass
            
            # Fallback: ISO-3 zurückgeben (falls Piper irgendwann 3-Letter nutzt)
            return iso3_code

    except Exception as e:
        print(f"Language detection failed ({output_format}): {e}")
        return "eng_Latn" if output_format == 'nllb' else "en"

 

app = Flask(__name__)

@app.route("/ping", methods=["GET"])
def ping_endpoint():
    return "pong", 200

@app.route("/vectorize", methods=["POST"])
def vectorize_endpoint():
    model = setup_vectorizer()
    try:
        text_to_vectorize = request.data.decode('utf-8')
        if not text_to_vectorize: return jsonify({"error": "Request body must contain text"}), 400
        instructional_text = "Represent this document for retrieval: " + text_to_vectorize
        embedding = model.encode(instructional_text)
        return jsonify({"vector": embedding.tolist()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@app.route("/languages", methods=["GET"])
def get_languages_endpoint():
    """
    Gibt eine Liste aller vom Modell unterstützten Sprachen zurück.
    Format: [{"code": "de", "name": "German"}, ...]
    """
    try:
        langs = _generate_language_list()
        return jsonify(langs)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route("/tts", methods=["POST"])
def tts_endpoint():
    try:
        data = request.get_json(force=True)
        text = data.get("text", "")

        if not text:
            return jsonify({"error": "No text provided"}), 400

        # Sprache erkennen
        detected_lang = predict_language(text, output_format='iso', max_length=2000)


        # Modell laden
        voice = get_or_load_piper_model(detected_lang)

        def generate():
            # --- WAV Header (einmalig am Anfang) ---
            sample_rate = voice.config.sample_rate
            channels = 1 
            width = 2
            datasize = 2147483647 # Max fake size
            
            header = bytes("RIFF", 'ascii')
            header += struct.pack('<I', 36 + datasize)
            header += bytes("WAVE", 'ascii')
            header += bytes("fmt ", 'ascii')
            header += struct.pack('<I', 16)
            header += struct.pack('<H', 1)
            header += struct.pack('<H', channels)
            header += struct.pack('<I', sample_rate)
            header += struct.pack('<I', sample_rate * channels * width)
            header += struct.pack('<H', channels * width)
            header += struct.pack('<H', width * 8)
            header += bytes("data", 'ascii')
            header += struct.pack('<I', datasize)       
            
            yield header

            # --- Chunking gegen OOM ---
            # Wir zerlegen den Text in kleine Häppchen (< 500 Zeichen)
            # und streamen das Audio kontinuierlich.
            text_chunks = smart_split_tts(text, max_chars=500)

            for text_part in text_chunks:
                # Synthesize für diesen Teil
                for chunk in voice.synthesize(text_part):
                    if hasattr(chunk, "data"):
                        yield chunk.data
                    elif hasattr(chunk, "audio_int16_bytes"):
                        yield chunk.audio_int16_bytes
                    elif hasattr(chunk, "bytes"):
                        yield chunk.bytes
                    elif hasattr(chunk, "audio"):
                        yield chunk.audio
                    elif isinstance(chunk, bytes):
                        yield chunk

        return Response(stream_with_context(generate()), mimetype="audio/wav")

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500  
  

@app.route("/translate", methods=["POST"])
def translate_endpoint():
    try:
        # force=True ist robuster gegen fehlende Header
        data = request.get_json(force=True) 
        text_to_translate = data.get("text", "")
        target_iso = data.get("target_lang", "en")
        
        if not text_to_translate:
            return jsonify({"error": "Request body must contain a 'text' field"}), 400

        # Ressourcen Check
        if 'codes' not in translation_resources or not translation_resources['codes']:
            return jsonify({"error": "Server configuration error: No language codes loaded."}), 500

        # Ziel-Sprache auflösen
        nllb_target_code = _resolve_nllb_code(target_iso, translation_resources['codes'])
        if not nllb_target_code:
            return jsonify({"error": f"Target language '{target_iso}' is not supported."}), 400

        # Zentrale Spracherkennung nutzen ---
        # Wir fordern das 'nllb' Format an
        nllb_source_code = predict_language(text_to_translate, output_format='nllb', max_length=2000)
            
        tok, mod = translation_resources['tok'], translation_resources['mod']
        tok.src_lang = nllb_source_code
        
        # Generator Funktion
        def generate_stream():
            meta = {
                "type": "meta", 
                "detected_source_lang": nllb_source_code, 
                "target_lang": nllb_target_code
            }
            yield json.dumps(meta) + "\n"

            # Chunking gegen OOM bei langen Texten
            text_chunks = chunk_text(text_to_translate, tok, max_tokens=900)

            for chunk in text_chunks:
                inputs = tok(
                    chunk, 
                    return_tensors="pt", 
                    padding=True, 
                    truncation=True, 
                    max_length=TRANSLATION_TOKEN_LIMIT
                )
                
                streamer = TextIteratorStreamer(tok, skip_special_tokens=True)
                
                generation_kwargs = dict(
                    **inputs, 
                    streamer=streamer, 
                    forced_bos_token_id=tok.convert_tokens_to_ids(nllb_target_code),
                    do_sample=False,              # Deterministisch
                    num_beams=1,                  # Schnell
                    max_length=TRANSLATION_TOKEN_LIMIT,
                    # Dynamische Länge + Puffer, aber nie über das Hard Limit
                    max_new_tokens=min(TRANSLATION_TOKEN_LIMIT, inputs['input_ids'].shape[-1]*2 + 50)
                )

                thread = Thread(target=mod.generate, kwargs=generation_kwargs)
                thread.start()

                for new_text in streamer:
                    if new_text: 
                        yield json.dumps({"type": "data", "chunk": new_text}) + "\n"
            
            yield json.dumps({"type": "done"}) + "\n"

        return Response(stream_with_context(generate_stream()), mimetype='application/x-ndjson')

    except Exception as e:
        # Nur im absoluten Fehlerfall loggen wir noch
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500    
  

#@app.route("/translate", methods=["POST"])
def ORIGINALtranslate_endpoint():
    try:
        data = request.get_json()
        if not data or "text" not in data:
            return jsonify({"error": "Request body must be JSON and contain a 'text' field"}), 400
        
        text_to_translate = data["text"]
        target_iso = data.get("target_lang", "en")
        
        # 1. Setup Resources
        nllb_target_code = _resolve_nllb_code(target_iso, translation_resources['codes'])
        if not nllb_target_code:
            return jsonify({"error": f"Target language '{target_iso}' is not supported."}), 400
            
        # 2. Language Detection (nur auf den ersten 2000 Zeichen für Speed)
        lbl = translation_resources['lid'].predict(text_to_translate.replace("\n", " ")[:2000])[0][0].replace("__label__", "")
        
        nllb_source_code = "eng_Latn"
        if lbl in translation_resources['codes']:
            nllb_source_code = lbl
        else:
            iso = lbl.split('_')[0]
            candidates = [iso]
            try:
                l = pycountry.languages.get(alpha_3=iso)
                if l and hasattr(l, 'macro_language'): candidates.append(l.macro_language.alpha_3)
            except: pass
            nllb_source_code = next((x for c in candidates for x in translation_resources['codes'] if x.startswith(c+"_")), "eng_Latn")
            
        tok, mod = translation_resources['tok'], translation_resources['mod']
        tok.src_lang = nllb_source_code
        
        # 3. Generator Funktion für Streaming
        def generate_stream():
            # Initial Metadata senden (für Frontend/Java)
            meta = {
                "type": "meta",
                "detected_source_lang": nllb_source_code,
                "target_lang": nllb_target_code
            }
            yield json.dumps(meta) + "\n"

            # Text Chunking um 1024 Token Limit zu umgehen
            text_chunks = chunk_text(text_to_translate, tok, max_tokens=900) # Puffer lassen

            if torch.cuda.is_available():
                translation_resources['mod'].to('cuda')

            for chunk in text_chunks:
                inputs = tok(
                    chunk, 
                    return_tensors="pt", 
                    padding=True, 
                    truncation=True, 
                    max_length=TRANSLATION_TOKEN_LIMIT,
                )
                
                # Wenn GPU verfügbar, Daten rüberschieben
                #if torch.cuda.is_available():
                #    inputs = {k: v.to('cuda') for k, v in inputs.items()}
                #    mod.to('cuda')

                streamer = TextIteratorStreamer(tok, skip_special_tokens=True)
                
                generation_kwargs = dict(
                    **inputs, 
                    streamer=streamer, 
                    forced_bos_token_id=tok.convert_tokens_to_ids(nllb_target_code),

                    do_sample=False,              # kein Sampling für stabilere, reproduzierbare Übersetzungen
                    num_beams=1,                  # Beam-Search (DEAKTIVIERT FÜR STREAMING)
                    length_penalty=1.0,           # variiere 0.8-1.2 je nach Länge/Tendenz
                    repetition_penalty=1.1,       # moderate Strafe gegen Wiederholung
                    no_repeat_ngram_size=3,       # optional; falls du N-Gram-Wiederholungen siehst
                    max_length=TRANSLATION_TOKEN_LIMIT, # Wir überschreiben den Modell-Default (200) mit unserem Limit (1024)
                    max_new_tokens=min(TRANSLATION_TOKEN_LIMIT, inputs['input_ids'].shape[-1]*2 + 50)
                )

                # Thread starten damit der Main-Thread streamen kann
                thread = Thread(target=mod.generate, kwargs=generation_kwargs)
                thread.start()

                for new_text in streamer:
                    # Jedes Wort/Token als JSON Chunk senden, leere Strings werden weggefiltert.
                    if new_text: 
                        yield json.dumps({"type": "data", "chunk": new_text}) + "\n"
            
            # Fertig-Signal
            yield json.dumps({"type": "done"}) + "\n"

        return Response(stream_with_context(generate_stream()), mimetype='application/x-ndjson')

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/reduce_dim/<string:model_type_or_dim>", methods=["POST"])
def umap_reduce_endpoint(model_type_or_dim):
    key_to_use = model_type_or_dim
    if key_to_use not in umap_models:
        try: key_to_use = int(model_type_or_dim)
        except ValueError: pass
    if key_to_use not in umap_models: return jsonify({"error": f"No UMAP model loaded for type/dimension '{model_type_or_dim}'"}), 404
    try:
        data = request.get_json()
        if not data or "vector" not in data: return jsonify({"error": "Request body must be JSON and contain a 'vector' field"}), 400
        vector_highD = np.array(data["vector"]).reshape(1, -1)
        model = umap_models[key_to_use]
        with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
            reduced_vector = model.transform(vector_highD)
        return jsonify({"reduced_vector": reduced_vector.flatten().tolist()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/cluster/<cluster_id>", methods=["POST"])
def cluster(cluster_id):
    if cluster_id not in hdbscan_model_paths: return jsonify({"error": f"No HDBSCAN model path found for cluster_id '{cluster_id}'."}), 404
    model = None 
    try:
        data = request.get_json()
        if not data or "vector" not in data: return jsonify({"error": "Request body must be JSON and contain a 'vector' field"}), 400
        vectorLowD_raw = np.array(data["vector"]).reshape(1, -1)
        normalizer = Normalizer(norm='l2')
        vectorLowD_normalized = normalizer.transform(vectorLowD_raw)
        model = joblib.load(hdbscan_model_paths[cluster_id])
        labels, probabilities = hdbscan.approximate_predict(model, vectorLowD_normalized)
        return jsonify({"label": labels[0].item(), "probability": probabilities[0].item()})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if model is not None:
            del model
            gc.collect()

if __name__ == "__main__":
    print("--- Pre-loading all models at startup ---")
    # Lade UMAP und entdecke HDBSCAN Modelle
    load_umap_models()
    load_hdbscan_models()
    
    # Lade das Übersetzungs- und Spracherkennungsmodell
    setup_translator()
    
    # Lade den Vectorizer
    setup_vectorizer()
    
    print("\n--- All models ready. Starting Flask server. ---")

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
