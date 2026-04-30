from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List

from .rag_pipeline import clean_answer_text, get_document_chunks, run_pipeline

DEFAULT_INDEX_NAME = "workspace.document_rag.document_rag_index"
DEFAULT_COLUMNS = ["chunk_id", "doc_id", "title", "url", "chunk_text"]
LOGGER = logging.getLogger(__name__)
TOKEN_PATTERN = re.compile(r"[a-zA-Z0-9]+")
KEYWORD_GROUPS = [
    {
        "unity", "catalog", "catalogs", "schema", "schemas", "permission", "permissions",
        "governance", "model", "models", "volume", "volumes",
        "catalogo", "catálogo", "catalogos", "catálogos", "esquema", "esquemas",
        "permissao", "permissão", "permissoes", "permissões", "governanca", "governança",
        "modelo", "modelos", "volume", "volumes",
    },
    {
        "vector", "search", "index", "indexes", "embedding", "embeddings", "endpoint", "endpoints",
        "vetorial", "indice", "índice", "indices", "índices", "busca", "embeddings", "endpoint", "endpoints",
    },
    {
        "change", "data", "feed", "delta", "source", "table", "tables", "enabled", "enable", "incremental",
        "mudanca", "mudança", "dados", "fonte", "tabela", "tabelas", "habilitado", "habilitada",
        "habilitar", "ativado", "ativada", "incremental",
    },
    {
        "app", "apps", "streamlit", "service", "principal", "auth", "authentication", "credential", "credentials",
        "aplicativo", "aplicativos", "servico", "serviço", "principal", "autenticacao", "autenticação",
        "credencial", "credenciais",
    },
    {
        "chunk", "chunks", "chunking", "semantic", "context", "paragraph", "paragraphs", "heading", "headings",
        "trecho", "trechos", "segmentacao", "segmentação", "semantico", "semântico", "contexto",
        "paragrafo", "parágrafo", "paragrafos", "parágrafos", "cabecalho", "cabeçalho", "cabecalhos", "cabeçalhos",
    },
    {
        "refresh", "refreshes", "pipeline", "pipelines", "modified", "rows", "downstream", "sync", "corpus",
        "atualizacao", "atualização", "atualizar", "pipeline", "pipelines", "modificado", "modificados",
        "linhas", "sincronizacao", "sincronização", "corpus",
    },
]


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


def build_query_vector(question: str) -> List[float]:
    tokens = [token.lower() for token in TOKEN_PATTERN.findall(question)]
    vector = [0.0] * len(KEYWORD_GROUPS)
    for token in tokens:
        for index, keywords in enumerate(KEYWORD_GROUPS):
            if token in keywords:
                vector[index] += 1.0

    if not any(vector):
        return [0.1] * len(KEYWORD_GROUPS)
    return vector


def build_vector_search_body(question: str) -> Dict[str, Any]:
    return {
        "columns": DEFAULT_COLUMNS,
        "num_results": 10,
        "query_vector": build_query_vector(question),
    }


def _chunk_rank(chunk_id: str) -> int:
    try:
        return int(chunk_id.rsplit("_", 1)[-1])
    except (TypeError, ValueError):
        return 9999


def _compose_grounded_answer(top_chunks: List[Dict[str, Any]]) -> str:
    if not top_chunks:
        return ""

    primary_doc_id = top_chunks[0]["doc_id"]
    primary_title = top_chunks[0]["title"]
    same_doc_chunks = [chunk for chunk in top_chunks if chunk["doc_id"] == primary_doc_id]
    same_doc_chunks.sort(key=lambda item: _chunk_rank(item.get("chunk_id", "")))

    cleaned_parts: List[str] = []
    for chunk in same_doc_chunks[:2]:
        cleaned = clean_answer_text(str(chunk["chunk_text"]).strip(), primary_title)
        if cleaned and cleaned not in cleaned_parts:
            cleaned_parts.append(cleaned)

    if cleaned_parts:
        return " ".join(cleaned_parts).strip()
    return clean_answer_text(str(top_chunks[0]["chunk_text"]).strip(), primary_title)


def _augment_primary_document_context(top_chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not top_chunks:
        return top_chunks

    primary_chunk = top_chunks[0]
    primary_doc_id = str(primary_chunk["doc_id"])
    primary_order = _chunk_rank(str(primary_chunk.get("chunk_id", "")))
    local_doc_chunks = get_document_chunks(primary_doc_id)
    if not local_doc_chunks:
        return top_chunks

    existing_chunk_ids = {str(chunk.get("chunk_id", "")) for chunk in top_chunks}
    augmented = list(top_chunks)
    for local_chunk in local_doc_chunks:
        local_order = int(local_chunk["chunk_order"])
        if local_order not in {primary_order - 1, primary_order, primary_order + 1}:
            continue
        local_chunk_id = str(local_chunk["chunk_id"])
        if local_chunk_id in existing_chunk_ids:
            continue
        augmented.append(
            {
                "chunk_id": local_chunk_id,
                "doc_id": str(local_chunk["doc_id"]),
                "title": str(local_chunk["title"]),
                "url": str(local_chunk["url"]),
                "chunk_text": str(local_chunk["chunk_text"]),
                "similarity": float(primary_chunk.get("similarity", 0.0)),
            }
        )
    return augmented


def normalize_vector_rows(payload: Any) -> List[Dict[str, Any]]:
    payload_dict = _payload_to_dict(payload)
    result = payload_dict.get("result", {})
    manifest = payload_dict.get("manifest") or result.get("manifest", {})
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
    host = os.getenv("DATABRICKS_HOST")
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

    if host and client_id and client_secret:
        workspace = WorkspaceClient(
            host=host,
            client_id=client_id,
            client_secret=client_secret,
        )
    else:
        workspace = WorkspaceClient()
    response = workspace.api_client.do(
        "POST",
        f"/api/2.0/vector-search/indexes/{index_name}/query",
        body=build_vector_search_body(question),
    )
    top_chunks = normalize_vector_rows(response)
    if not top_chunks:
        raise RuntimeError("Vector Search returned no results.")

    top_chunk = top_chunks[0]
    answer = _compose_grounded_answer(_augment_primary_document_context(top_chunks))
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
            LOGGER.exception("Primary Vector Search query failed; falling back to local retrieval.")
            fallback = run_pipeline(question)
            fallback["runtime_mode"] = "local_retrieval_fallback_after_vector_error"
            fallback["vector_error"] = str(exc)
            return fallback
    return run_pipeline(question)
