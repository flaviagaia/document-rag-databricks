from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from src.rag_pipeline import run_pipeline
from src.runtime_query import in_databricks_runtime, run_hybrid_query


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

    def test_runtime_detection_defaults_to_local(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(in_databricks_runtime())

    def test_hybrid_query_falls_back_when_vector_search_fails(self) -> None:
        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://example.databricks.com"}, clear=True):
            with patch("src.runtime_query.search_with_vector_search", side_effect=RuntimeError("index warming")):
                result = run_hybrid_query("How does Change Data Feed help a vector index stay fresh?")
        self.assertEqual(result["top_doc_id"], "DOC-1002")
        self.assertEqual(result["runtime_mode"], "local_retrieval_fallback_after_vector_error")
        self.assertIn("index warming", result["vector_error"])


if __name__ == "__main__":
    unittest.main()
