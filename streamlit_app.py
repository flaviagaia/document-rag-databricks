from __future__ import annotations

from typing import Any, Dict

import streamlit as st

from src.rag_pipeline import DEFAULT_QUERY
from src.runtime_query import run_hybrid_query

APP_TITLE = "Document RAG Databricks"


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
    result = run_hybrid_query(question)
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
