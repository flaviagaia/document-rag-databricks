from __future__ import annotations

import unittest

from src.rag_pipeline import run_pipeline


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


if __name__ == "__main__":
    unittest.main()
