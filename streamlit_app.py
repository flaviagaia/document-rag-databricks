from __future__ import annotations

from typing import Any, Dict

import streamlit as st

from src.runtime_query import run_hybrid_query

APP_TITLE = "Assistente de Documentos"
APP_SUBTITLE = "Faça uma pergunta em linguagem natural e encontre a resposta com base nos documentos indexados no lakehouse."
EXAMPLE_QUESTIONS = [
    "O que precisa estar habilitado antes de criar um índice vetorial padrão?",
    "Como o Change Data Feed ajuda a manter o índice atualizado?",
    "Qual é o papel do Unity Catalog nesse fluxo?",
]


def _similarity_label(score: float) -> str:
    if score >= 0.75:
        return "Muito alta"
    if score >= 0.5:
        return "Alta"
    if score >= 0.3:
        return "Média"
    return "Baixa"


def _render_summary(result: Dict[str, Any]) -> None:
    left, middle, right = st.columns(3)
    left.metric("Documento principal", result["top_doc_id"])
    middle.metric("Confiança do melhor trecho", _similarity_label(result["top_similarity"]))
    right.metric("Trechos encontrados", str(len(result["top_chunks"])))


def _render_chunk_card(rank: int, chunk: Dict[str, Any]) -> None:
    with st.container(border=True):
        st.markdown(f"**Evidência {rank}: {chunk['title']}**")
        st.caption(f"Documento {chunk['doc_id']}")
        st.write(chunk["chunk_text"])
        footer = f"Confiança: {_similarity_label(chunk['similarity'])}"
        if chunk.get("url"):
            footer += f" • Fonte: {chunk['url']}"
        st.caption(footer)


st.set_page_config(page_title=APP_TITLE, page_icon="📚", layout="wide")

st.title(APP_TITLE)
st.caption(APP_SUBTITLE)

with st.sidebar:
    st.subheader("Perguntas de exemplo")
    for example in EXAMPLE_QUESTIONS:
        st.caption(f"• {example}")
    st.divider()
    st.subheader("Como usar")
    st.caption("1. Digite sua pergunta.")
    st.caption("2. Leia a resposta sugerida.")
    st.caption("3. Confira os trechos de apoio logo abaixo.")

default_question = "O que precisa estar habilitado antes de criar um índice vetorial padrão?"
question = st.text_input("Digite sua pergunta", value=default_question, placeholder="Ex.: Como manter o índice vetorial atualizado?")

search_clicked = st.button("Buscar resposta", type="primary")

if search_clicked or question:
    result = run_hybrid_query(question)
    top_chunks = result["top_chunks"]

    _render_summary(result)

    st.subheader("Resposta sugerida")
    st.info(result["answer"])

    for i, chunk in enumerate(top_chunks, start=1):
        _render_chunk_card(i, chunk)

    if result.get("vector_error"):
        st.warning("Não foi possível consultar a busca principal neste momento. Mesmo assim, exibimos uma resposta alternativa para você continuar a consulta.")
