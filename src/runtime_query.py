from __future__ import annotations

import os
from typing import Any, Dict, List

from .rag_pipeline import run_pipeline

DEFAULT_INDEX_NAME = "workspace.document_rag.document_rag_index"
DEFAULT_COLUMNS = ["chunk_id", "doc_id", "title", "url", "chunk_text"]


def in_databricks_runtime() -> bool:
    return bool(
        os.getenv("DATABRICKS_RUNTIME_VERSION")
        or os.getenv("DB_IS_JOB_CLUSTER")
        or os.getenv("DATABRICKS_HOST")
        or os.getenv("DATABRICKS_CLIENT_ID")
        or os.getenv("DATABRICKS_CLIENT_SECRET")
        or os.getenv("VECTOR_SEARCH_INDEX")
    )


def _payload_to_dict(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if hasattr(payload, "as_dict"):
        return payload.as_dict()
    result = getattr(payload, "result", None)
    manifest = getattr(result, "manifest", None) if result is not None else None
    return {
        "result": {
            "manifest": {
                "columns": [
                    {"name": getattr(column, "name", "")}
                    for column in getattr(manifest, "columns", [])
                ]
            }
            if manifest is not None
            else {},
            "data_array": getattr(result, "data_array", []) if result is not None else [],
        }
    }


def normalize_vector_rows(payload: Any) -> List[Dict[str, Any]]:
    payload_dict = _payload_to_dict(payload)
    result = payload_dict.get("result", {})
    manifest = result.get("manifest", {})
    columns = [
        column.get("name", f"column_{index}")
        for index, column in enumerate(manifest.get("columns", []))
    ]
    rows = result.get("data_array", [])

    normalized: List[Dict[str, Any]] = []
    for raw_row in rows:
        item = dict(zip(columns, raw_row))
        score = item.get("score", 0.0)
        try:
            similarity = round(float(score), 4)
        except (TypeError, ValueError):
            similarity = 0.0
        normalized.append(
            {
                "chunk_id": item.get("chunk_id", ""),
                "doc_id": item.get("doc_id", ""),
                "title": item.get("title", "Untitled chunk"),
                "url": item.get("url", ""),
                "chunk_text": item.get("chunk_text", ""),
                "similarity": similarity,
            }
        )
    return normalized


def search_with_vector_search(question: str) -> Dict[str, Any]:
    from databricks.sdk import WorkspaceClient

    index_name = os.getenv("VECTOR_SEARCH_INDEX", DEFAULT_INDEX_NAME)
    workspace = WorkspaceClient()
    response = workspace.vector_search_indexes.query_index(
        index_name=index_name,
        query_text=question,
        num_results=3,
        columns=DEFAULT_COLUMNS,
    )
    top_chunks = normalize_vector_rows(response)
    if not top_chunks:
        raise RuntimeError("Vector Search returned no results.")

    top_chunk = top_chunks[0]
    answer = top_chunk["chunk_text"].strip()
    return {
        "dataset_source": "workspace.document_rag.gold_vector_index_source",
        "runtime_mode": "databricks_vector_search",
        "document_count": None,
        "chunk_count": None,
        "query": question,
        "top_doc_id": top_chunk["doc_id"],
        "top_chunk_id": top_chunk["chunk_id"],
        "top_similarity": top_chunk["similarity"],
        "answer": answer,
        "top_chunks": top_chunks,
        "report_artifact": index_name,
    }


def run_hybrid_query(question: str) -> Dict[str, Any]:
    if in_databricks_runtime():
        try:
            return search_with_vector_search(question)
        except Exception as exc:  # pragma: no cover - defensive runtime fallback
            fallback = run_pipeline(question)
            fallback["runtime_mode"] = "local_retrieval_fallback_after_vector_error"
            fallback["vector_error"] = str(exc)
            return fallback
    return run_pipeline(question)
