from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from src.rag_pipeline import run_pipeline
from src.runtime_query import (
    _compose_grounded_answer,
    build_query_vector,
    in_databricks_runtime,
    normalize_vector_rows,
    run_hybrid_query,
)


class DocumentRAGDatabricksTestCase(unittest.TestCase):
    def test_pipeline_contract(self) -> None:
        result = run_pipeline()
        self.assertEqual(result["dataset_source"], "watsonxdocsqa_style_local_sample")
        self.assertEqual(result["document_count"], 6)
        self.assertEqual(result["qa_count"], 2)
        self.assertGreaterEqual(result["chunk_count"], 6)
        self.assertEqual(result["runtime_mode"], "local_retrieval_fallback")
        self.assertIsNotNone(result["top_doc_id"])
        self.assertGreaterEqual(result["top_similarity"], 0.15)
        self.assertEqual(len(result["top_chunks"]), 3)

    def test_query_hits_vector_search_document(self) -> None:
        result = run_pipeline("What has to be enabled before a standard vector search index can use a Delta source table?")
        self.assertEqual(result["top_doc_id"], "DOC-1002")
        self.assertNotIn("The most relevant answer is grounded in", result["answer"])

    def test_runtime_detection_defaults_to_local(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(in_databricks_runtime())

    def test_runtime_detection_accepts_app_service_principal_env(self) -> None:
        with patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "client-id-from-app"}, clear=True):
            self.assertTrue(in_databricks_runtime())

    def test_query_vector_maps_vector_search_question_to_expected_dimensions(self) -> None:
        vector = build_query_vector("What must be enabled before creating a standard vector search index from a Delta table?")
        self.assertGreater(vector[1], 0.0)
        self.assertGreater(vector[2], 0.0)

    def test_query_vector_maps_portuguese_question_to_expected_dimensions(self) -> None:
        vector = build_query_vector("O que precisa estar habilitado antes de criar um índice vetorial padrão?")
        self.assertGreater(vector[1], 0.0)
        self.assertGreater(vector[2], 0.0)

    def test_compose_grounded_answer_merges_same_document_chunks(self) -> None:
        answer = _compose_grounded_answer(
            [
                {
                    "chunk_id": "DOC-1002_chunk_2",
                    "doc_id": "DOC-1002",
                    "title": "Create a vector search index",
                    "chunk_text": "be enabled on the source table.",
                },
                {
                    "chunk_id": "DOC-1002_chunk_1",
                    "doc_id": "DOC-1002",
                    "title": "Create a vector search index",
                    "chunk_text": "# Create a vector search index A vector search index is created from a Delta table containing content, metadata, and embeddings. For standard endpoints, Change Data Feed must",
                },
            ]
        )
        self.assertIn("Change Data Feed must", answer)
        self.assertIn("be enabled on the source table.", answer)

    def test_hybrid_query_falls_back_when_vector_search_fails(self) -> None:
        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://example.databricks.com"}, clear=True):
            with patch("src.runtime_query.search_with_vector_search", side_effect=RuntimeError("index warming")):
                result = run_hybrid_query("How does Change Data Feed help a vector index stay fresh?")
        self.assertEqual(result["top_doc_id"], "DOC-1002")
        self.assertEqual(result["runtime_mode"], "local_retrieval_fallback_after_vector_error")
        self.assertIn("index warming", result["vector_error"])

    def test_normalize_vector_rows_accepts_sdk_like_response(self) -> None:
        class FakePayload:
            def as_dict(self) -> dict[str, object]:
                return {
                    "result": {
                        "manifest": {
                            "columns": [
                                {"name": "chunk_id"},
                                {"name": "doc_id"},
                                {"name": "title"},
                                {"name": "url"},
                                {"name": "chunk_text"},
                                {"name": "score"},
                            ]
                        },
                        "data_array": [
                            [
                                "DOC-1002_chunk_1",
                                "DOC-1002",
                                "Create a vector search index",
                                "https://docs.example.com/vector-search/create-index",
                                "Change Data Feed must be enabled on the source table.",
                                0.81234,
                            ]
                        ],
                    }
                }

        rows = normalize_vector_rows(FakePayload())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["doc_id"], "DOC-1002")
        self.assertEqual(rows[0]["similarity"], 0.8123)


if __name__ == "__main__":
    unittest.main()
