from __future__ import annotations

import os
from typing import Any, Dict, List

import streamlit as st

from src.rag_pipeline import DEFAULT_QUERY, run_pipeline

APP_TITLE = "Document RAG Databricks"
DEFAULT_INDEX_NAME = "workspace.document_rag.document_rag_index"
DEFAULT_COLUMNS = ["chunk_id", "doc_id", "title", "url", "chunk_text"]


def _in_databricks_runtime() -> bool:
    return bool(os.getenv("DATABRICKS_RUNTIME_VERSION") or os.getenv("DB_IS_JOB_CLUSTER") or os.getenv("DATABRICKS_HOST"))


def _normalize_vector_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = payload.get("result", {})
    manifest = result.get("manifest", {})
    columns = [column.get("name", f"column_{index}") for index, column in enumerate(manifest.get("columns", []))]
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


def _search_with_vector_search(question: str) -> Dict[str, Any]:
    from databricks.sdk import WorkspaceClient

    index_name = os.getenv("VECTOR_SEARCH_INDEX", DEFAULT_INDEX_NAME)
    workspace = WorkspaceClient()
    response = workspace.vector_search_indexes.query_index(
        index_name=index_name,
        query_text=question,
        num_results=3,
        columns=DEFAULT_COLUMNS,
    )
    top_chunks = _normalize_vector_rows(response)
    if not top_chunks:
        raise RuntimeError("Vector Search returned no results.")

    top_chunk = top_chunks[0]
    return {
        "dataset_source": "workspace.document_rag.gold_vector_index_source",
        "runtime_mode": "databricks_vector_search",
        "document_count": None,
        "chunk_count": None,
        "query": question,
        "top_doc_id": top_chunk["doc_id"],
        "top_chunk_id": top_chunk["chunk_id"],
        "top_similarity": top_chunk["similarity"],
        "answer": (
            f"The strongest grounded answer is in {top_chunk['title']}. "
            f"The retrieved chunk says: {top_chunk['chunk_text']}"
        ),
        "top_chunks": top_chunks,
        "report_artifact": index_name,
    }


def _run_app_query(question: str) -> Dict[str, Any]:
    if _in_databricks_runtime():
        try:
            return _search_with_vector_search(question)
        except Exception as exc:  # pragma: no cover - defensive runtime fallback
            fallback = run_pipeline(question)
            fallback["runtime_mode"] = "local_retrieval_fallback_after_vector_error"
            fallback["vector_error"] = str(exc)
            return fallback
    return run_pipeline(question)


def _render_summary(result: Dict[str, Any]) -> None:
    left, middle, right = st.columns(3)
    left.metric("Top result", result["top_doc_id"])
    middle.metric("Similarity", f"{result['top_similarity']:.4f}")
    right.metric("Mode", result["runtime_mode"])


def _render_chunk_card(rank: int, chunk: Dict[str, Any]) -> None:
    with st.container(border=True):
        st.markdown(f"**#{rank} {chunk['title']}**")
        st.caption(f"Document {chunk['doc_id']} • chunk {chunk['chunk_id']}")
        st.write(chunk["chunk_text"])
        footer = f"Similarity: {chunk['similarity']:.4f}"
        if chunk.get("url"):
            footer += f" • Source: {chunk['url']}"
        st.caption(footer)


st.set_page_config(page_title=APP_TITLE, page_icon="📚", layout="wide")

st.title(APP_TITLE)
st.caption("Ask a question about the document collection and retrieve grounded evidence from the lakehouse-backed sample.")

question = st.text_input("Question", value=DEFAULT_QUERY)

if st.button("Search", type="primary") or question:
    result = _run_app_query(question)
    top_chunks = result["top_chunks"]

    _render_summary(result)

    left, right = st.columns([1.2, 1])

    with left:
        st.subheader("Answer")
        st.write(result["answer"])
        st.subheader("Retrieved chunks")
        for i, chunk in enumerate(top_chunks, start=1):
            _render_chunk_card(i, chunk)

    with right:
        st.subheader("Run summary")
        summary = {
            "dataset_source": result["dataset_source"],
            "runtime_mode": result["runtime_mode"],
            "query": result["query"],
            "top_doc_id": result["top_doc_id"],
            "report_artifact": result["report_artifact"],
        }
        if result.get("vector_error"):
            summary["vector_error"] = result["vector_error"]
        st.json(summary)

        if result.get("document_count") is not None:
            st.caption(f"Documents available in local sample: {result['document_count']}")
        if result.get("chunk_count") is not None:
            st.caption(f"Chunks available in local sample: {result['chunk_count']}")
