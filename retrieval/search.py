"""Pure ranking logic for the retrieval Lambda: cosine similarity and top-k selection.

Kept dependency-free (no boto3, no AWS clients) so it can be unit tested in
isolation from S3 and Bedrock, and so the similarity computation stays
auditable in any production review.
"""
from __future__ import annotations

import math
from typing import Any, Sequence


def cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    """Return the cosine similarity between two equal-length vectors.

    Returns 0.0 if either vector has zero magnitude, to avoid a division by zero.
    """
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def rank_chunks(
    query_embedding: Sequence[float], chunks: list[dict[str, Any]], top_k: int
) -> list[dict[str, Any]]:
    """Score ``chunks`` against ``query_embedding`` and return the top ``top_k`` by similarity."""
    scored = [
        {
            "chunk_id": chunk["chunk_id"],
            "doc_path": chunk["doc_path"],
            "title": chunk.get("title"),
            "summary": chunk.get("summary"),
            "chunk_text": chunk["chunk_text"],
            "score": round(cosine_similarity(query_embedding, chunk["embedding"]), 4),
        }
        for chunk in chunks
    ]
    scored.sort(key=lambda item: item["score"], reverse=True)
    return scored[:top_k]
