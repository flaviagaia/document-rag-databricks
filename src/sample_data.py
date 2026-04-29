from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, List


ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT_DIR / "data" / "raw"


DOCUMENTS: List[Dict[str, object]] = [
    {
        "doc_id": "DOC-1001",
        "url": "https://docs.example.com/workspaces/unity-catalog-overview",
        "title": "Unity Catalog overview",
        "document": (
            "Unity Catalog centralizes governance for data and AI assets. It provides a unified model for "
            "catalogs, schemas, tables, volumes, models, and permissions across the lakehouse."
        ),
        "md_document": (
            "# Unity Catalog overview\n\nUnity Catalog centralizes governance for data and AI assets. "
            "It provides a unified model for catalogs, schemas, tables, volumes, models, and permissions "
            "across the lakehouse."
        ),
    },
    {
        "doc_id": "DOC-1002",
        "url": "https://docs.example.com/vector-search/create-index",
        "title": "Create a vector search index",
        "document": (
            "A vector search index is created from a Delta table containing content, metadata, and embeddings. "
            "For standard endpoints, Change Data Feed must be enabled on the source table."
        ),
        "md_document": (
            "# Create a vector search index\n\nA vector search index is created from a Delta table containing "
            "content, metadata, and embeddings. For standard endpoints, Change Data Feed must be enabled on "
            "the source table."
        ),
    },
    {
        "doc_id": "DOC-1003",
        "url": "https://docs.example.com/delta/change-data-feed",
        "title": "Delta Change Data Feed",
        "document": (
            "Change Data Feed records inserts, updates, and deletes on Delta tables after the property is enabled. "
            "It supports incremental ETL, downstream sync, and audit scenarios."
        ),
        "md_document": (
            "# Delta Change Data Feed\n\nChange Data Feed records inserts, updates, and deletes on Delta tables "
            "after the property is enabled. It supports incremental ETL, downstream sync, and audit scenarios."
        ),
    },
    {
        "doc_id": "DOC-1004",
        "url": "https://docs.example.com/apps/streamlit-databricks-apps",
        "title": "Build a Streamlit app in Databricks Apps",
        "document": (
            "Databricks Apps can host Streamlit applications that read Unity Catalog tables or query vector search indexes. "
            "The app service principal needs the right permissions on the underlying resources."
        ),
        "md_document": (
            "# Build a Streamlit app in Databricks Apps\n\nDatabricks Apps can host Streamlit applications that "
            "read Unity Catalog tables or query vector search indexes. The app service principal needs the right "
            "permissions on the underlying resources."
        ),
    },
    {
        "doc_id": "DOC-1005",
        "url": "https://docs.example.com/rag/chunking-best-practices",
        "title": "Chunking best practices for document RAG",
        "document": (
            "Chunking should preserve semantic boundaries such as headings and paragraphs. Overly large chunks reduce precision, "
            "while very small chunks may lose context needed for grounded answering."
        ),
        "md_document": (
            "# Chunking best practices for document RAG\n\nChunking should preserve semantic boundaries such as headings "
            "and paragraphs. Overly large chunks reduce precision, while very small chunks may lose context needed for grounded answering."
        ),
    },
    {
        "doc_id": "DOC-1006",
        "url": "https://docs.example.com/rag/streaming-incremental-index-refresh",
        "title": "Incremental refresh for RAG indexes",
        "document": (
            "Incremental refresh pipelines can use Delta Change Data Feed to identify modified rows and update downstream "
            "vector index source tables without rebuilding the full corpus."
        ),
        "md_document": (
            "# Incremental refresh for RAG indexes\n\nIncremental refresh pipelines can use Delta Change Data Feed to "
            "identify modified rows and update downstream vector index source tables without rebuilding the full corpus."
        ),
    },
]


QUESTION_ANSWERS: List[Dict[str, object]] = [
    {
        "question_id": "QA-1001",
        "question": "What needs to be enabled on a Delta table before using it as the source for a standard vector search index?",
        "correct_answer": "Change Data Feed must be enabled on the Delta source table.",
        "ground_truth_doc_ids": "DOC-1002|DOC-1003",
    },
    {
        "question_id": "QA-1002",
        "question": "Why is chunking important in document RAG pipelines?",
        "correct_answer": "Chunking helps preserve semantic boundaries and improves retrieval precision while keeping enough context for grounded answers.",
        "ground_truth_doc_ids": "DOC-1005",
    },
]


REFERENCE = {
    "dataset_inspiration": "ibm-research/watsonxDocsQA",
    "license": "apache-2.0",
    "design_note": (
        "This sample mirrors an enterprise documentation corpus suitable for Databricks document RAG. "
        "The production architecture uses Delta tables, Unity Catalog, Mosaic AI Vector Search, and Streamlit inside Databricks Apps."
    ),
}


def build_sample_dataset() -> Dict[str, str]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    docs_path = RAW_DIR / "documents.csv"
    qa_path = RAW_DIR / "qa_benchmark.csv"
    ref_path = RAW_DIR / "dataset_reference.json"

    with docs_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["doc_id", "url", "title", "document", "md_document"],
        )
        writer.writeheader()
        writer.writerows(DOCUMENTS)

    with qa_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["question_id", "question", "correct_answer", "ground_truth_doc_ids"],
        )
        writer.writeheader()
        writer.writerows(QUESTION_ANSWERS)

    with ref_path.open("w", encoding="utf-8") as json_file:
        json.dump(REFERENCE, json_file, indent=2)

    return {
        "dataset_source": "watsonxdocsqa_style_local_sample",
        "documents_path": str(docs_path),
        "qa_path": str(qa_path),
        "reference_path": str(ref_path),
    }
