from __future__ import annotations

from typing import Any, Dict

import streamlit as st

from src.runtime_query import run_hybrid_query

APP_TITLE = "Assistente de Documentos"
APP_SUBTITLE = "Faça uma pergunta em linguagem natural e encontre a resposta com base nos documentos indexados no lakehouse."
DATASET_EXPLANATION = (
    "Este assistente consulta uma base documental técnica sobre Databricks, com conteúdos de referência "
    "sobre Unity Catalog, Vector Search, Delta Change Data Feed, Streamlit em Databricks Apps e boas práticas de RAG."
)
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
    right.metric(
        "Modo de busca",
        "Principal" if result["runtime_mode"] == "databricks_vector_search" else "Alternativo",
    )


st.set_page_config(page_title=APP_TITLE, page_icon="📚", layout="wide")

st.title(APP_TITLE)
st.caption(APP_SUBTITLE)
st.info(DATASET_EXPLANATION)

with st.sidebar:
    st.subheader("Perguntas de exemplo")
    for example in EXAMPLE_QUESTIONS:
        st.caption(f"• {example}")
    st.divider()
    st.subheader("Como usar")
    st.caption("1. Digite sua pergunta.")
    st.caption("2. Leia a resposta sugerida.")
    st.caption("3. Ajuste a pergunta se quiser refinar a resposta.")

default_question = "O que precisa estar habilitado antes de criar um índice vetorial padrão?"
if "question_input" not in st.session_state:
    st.session_state["question_input"] = default_question
if "last_result" not in st.session_state:
    st.session_state["last_result"] = None

with st.form("document_search_form"):
    question = st.text_input(
        "Digite sua pergunta",
        key="question_input",
        placeholder="Ex.: Como manter o índice vetorial atualizado?",
    )
    search_clicked = st.form_submit_button("Buscar resposta", type="primary")

if search_clicked:
    normalized_question = question.strip()
    if normalized_question:
        st.session_state["last_result"] = run_hybrid_query(normalized_question)
    else:
        st.session_state["last_result"] = None
        st.warning("Digite uma pergunta para continuar.")

result = st.session_state["last_result"]

if result:
    _render_summary(result)

    st.subheader("Resposta sugerida")
    st.info(result["answer"])

    if result.get("vector_error"):
        st.warning("Não foi possível consultar a busca principal neste momento. Mesmo assim, exibimos uma resposta alternativa para você continuar a consulta.")
