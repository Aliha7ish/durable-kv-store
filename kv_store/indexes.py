"""Indexes on value: full-text search and optional word embedding."""

import re
import threading
from typing import Any, Optional

# Optional: word embedding; use simple bag-of-words + hash for embedding if no deps
try:
    # Placeholder: no heavy deps by default; can install sentence-transformers for real embeddings
    _HAS_EMBEDDING = False
except Exception:
    _HAS_EMBEDDING = False


def _tokenize(text: str) -> list[str]:
    """Simple tokenization: lowercase, split on non-alphanumeric."""
    text = (text if isinstance(text, str) else str(text)).lower()
    return re.findall(r"\b\w+\b", text)


class FullTextIndex:
    """Inverted index: word -> set of key ids. Values are tokenized for full-text search."""

    def __init__(self):
        self._word_to_keys: dict[str, set[str]] = {}
        self._lock = threading.RLock()

    def index_value(self, key: str, value: Any) -> None:
        """Index a key-value pair. Value is tokenized (string)."""
        with self._lock:
            # Remove old postings for this key
            for keys in self._word_to_keys.values():
                keys.discard(key)
            words = _tokenize(str(value))
            for w in words:
                self._word_to_keys.setdefault(w, set()).add(key)

    def remove_key(self, key: str) -> None:
        """Remove key from index."""
        with self._lock:
            for keys in list(self._word_to_keys.values()):
                keys.discard(key)
            self._word_to_keys = {w: k for w, k in self._word_to_keys.items() if k}

    def search(self, query: str) -> list[str]:
        """Full-text search: return keys whose value contains all query words (AND)."""
        with self._lock:
            words = _tokenize(query)
            if not words:
                return []
            sets = [self._word_to_keys.get(w, set()) for w in words]
            if not sets:
                return []
            result = sets[0]
            for s in sets[1:]:
                result = result & s
            return list(result)


class WordEmbeddingIndex:
    """Simple 'embedding' index: bag-of-words hashed to a fixed-size vector for similarity."""

    def __init__(self, dim: int = 64):
        self._dim = dim
        self._key_to_vec: dict[str, list[float]] = {}
        self._lock = threading.RLock()

    def _to_vec(self, text: str) -> list[float]:
        """Bag-of-words hashed to dim dimensions (no external deps)."""
        words = _tokenize(text)
        vec = [0.0] * self._dim
        for w in words:
            h = hash(w) % self._dim
            vec[h % self._dim] += 1.0
        norm = (sum(x * x for x in vec)) ** 0.5
        if norm > 0:
            vec = [x / norm for x in vec]
        return vec

    def index_value(self, key: str, value: Any) -> None:
        with self._lock:
            self._key_to_vec[key] = self._to_vec(str(value))

    def remove_key(self, key: str) -> None:
        with self._lock:
            self._key_to_vec.pop(key, None)

    def similarity(self, a: list[float], b: list[float]) -> float:
        """Dot product (cosine sim when vectors are normalized)."""
        return sum(x * y for x, y in zip(a, b))

    def search_similar(self, query: str, top_k: int = 10) -> list[tuple[str, float]]:
        """Return keys with values most similar to query (by bag-of-words 'embedding')."""
        with self._lock:
            qvec = self._to_vec(query)
            scores = [(k, self.similarity(qvec, v)) for k, v in self._key_to_vec.items()]
            scores.sort(key=lambda x: -x[1])
            return scores[:top_k]
