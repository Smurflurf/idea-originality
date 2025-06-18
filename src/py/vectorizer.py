from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')

def encode_text(text: str) -> list[float]:
    if not text or not isinstance(text, str):
        return []
    vector = model.encode(text)
    return vector.tolist()

def encode_batch(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    vectors = model.encode(texts)
    return [v.tolist() for v in vectors]