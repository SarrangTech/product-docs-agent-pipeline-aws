"""Unit tests for the retrieval ranking logic."""
import pytest

from retrieval.search import cosine_similarity, rank_chunks


def test_cosine_similarity_identical_vectors_returns_one() -> None:
    vector = [1.0, 2.0, 3.0]
    assert cosine_similarity(vector, vector) == pytest.approx(1.0)


def test_cosine_similarity_orthogonal_vectors_returns_zero() -> None:
    assert cosine_similarity([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)


def test_cosine_similarity_zero_vector_returns_zero() -> None:
    assert cosine_similarity([0.0, 0.0], [1.0, 1.0]) == 0.0


def test_rank_chunks_returns_top_k_in_similarity_order() -> None:
    query = [1.0, 0.0]
    chunks = [
        {"chunk_id": "a", "doc_path": "a.md", "chunk_text": "a", "embedding": [1.0, 0.0]},
        {"chunk_id": "b", "doc_path": "b.md", "chunk_text": "b", "embedding": [0.0, 1.0]},
        {"chunk_id": "c", "doc_path": "c.md", "chunk_text": "c", "embedding": [0.7, 0.7]},
    ]

    results = rank_chunks(query, chunks, top_k=2)

    assert [result["chunk_id"] for result in results] == ["a", "c"]
    assert results[0]["score"] == 1.0
