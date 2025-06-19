from sentence_transformers import SentenceTransformer, models
import time

MODEL_NAME = "dwzhu/e5-base-4k"

word_embedding_model = models.Transformer(MODEL_NAME)
pooling_model = models.Pooling(word_embedding_model.get_word_embedding_dimension())
model = SentenceTransformer(modules=[word_embedding_model, pooling_model])


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