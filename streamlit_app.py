from __future__ import annotations

import streamlit as st

from src.rag_pipeline import DEFAULT_QUERY, run_pipeline


st.set_page_config(page_title="Document RAG Databricks", page_icon="📚", layout="wide")

st.title("Document RAG Demo")
st.caption("Ask a question and retrieve the most relevant chunk from the document lakehouse sample.")

question = st.text_input("Question", value=DEFAULT_QUERY)

if st.button("Search", type="primary") or question:
    result = run_pipeline(question)
    top_chunks = result["top_chunks"]

    col1, col2, col3 = st.columns(3)
    col1.metric("Documents", result["document_count"])
    col2.metric("Chunks", result["chunk_count"])
    col3.metric("Top Similarity", f"{result['top_similarity']:.4f}")

    left, right = st.columns([1.2, 1])

    with left:
        st.subheader("Answer")
        st.write(result["answer"])
        st.subheader("Retrieved chunks")
        for i, chunk in enumerate(top_chunks, start=1):
            with st.container(border=True):
                st.markdown(
                    f"**#{i} {chunk['title']}**  \n"
                    f"`doc_id = {chunk['doc_id']}`  \n"
                    f"`chunk_id = {chunk['chunk_id']}`  \n"
                    f"`similarity = {chunk['similarity']}`"
                )
                st.write(chunk["chunk_text"])

    with right:
        st.subheader("Run summary")
        st.json(
            {
                "dataset_source": result["dataset_source"],
                "runtime_mode": result["runtime_mode"],
                "query": result["query"],
                "top_doc_id": result["top_doc_id"],
                "report_artifact": result["report_artifact"],
            }
        )
