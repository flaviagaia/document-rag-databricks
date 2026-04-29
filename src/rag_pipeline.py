from __future__ import annotations

import csv
import json
import math
import re
from pathlib import Path
from typing import Dict, List

from .sample_data import build_sample_dataset


ROOT_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT_DIR / "data" / "processed"

TOKEN_PATTERN = re.compile(r"[a-zA-Z0-9]+")
DEFAULT_QUERY = "What must be enabled on a Delta table before creating a standard vector search index?"


def _tokenize(text: str) -> List[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(text)]


def _load_rows(path: str) -> List[Dict[str, str]]:
    with Path(path).open("r", encoding="utf-8", newline="") as csv_file:
        return list(csv.DictReader(csv_file))


def _chunk_documents(rows: List[Dict[str, str]], chunk_size: int = 28) -> List[Dict[str, object]]:
    chunks: List[Dict[str, object]] = []
    for row in rows:
        tokens = row["md_document"].split()
        for index in range(0, len(tokens), chunk_size):
            part = tokens[index:index + chunk_size]
            chunk_number = index // chunk_size + 1
            chunks.append(
                {
                    "chunk_id": f"{row['doc_id']}_chunk_{chunk_number}",
                    "doc_id": row["doc_id"],
                    "title": row["title"],
                    "url": row["url"],
                    "chunk_order": chunk_number,
                    "chunk_text": " ".join(part),
                }
            )
    return chunks


def _build_tfidf(chunks: List[Dict[str, object]]) -> List[Dict[str, float]]:
    tokenized_docs = [_tokenize(str(chunk["chunk_text"])) for chunk in chunks]
    vocab = sorted({token for doc in tokenized_docs for token in doc})
    doc_freq = {token: 0 for token in vocab}
    for doc in tokenized_docs:
        for token in set(doc):
            doc_freq[token] += 1

    doc_count = len(tokenized_docs)
    vectors: List[Dict[str, float]] = []
    for doc in tokenized_docs:
        counts: Dict[str, int] = {}
        for token in doc:
            counts[token] = counts.get(token, 0) + 1
        length = max(len(doc), 1)
        vector: Dict[str, float] = {}
        for token, count in counts.items():
            tf = count / length
            idf = math.log((1 + doc_count) / (1 + doc_freq[token])) + 1
            vector[token] = tf * idf
        vectors.append(vector)
    return vectors


def _cosine(left: Dict[str, float], right: Dict[str, float]) -> float:
    if not left or not right:
        return 0.0
    shared = set(left).intersection(right)
    numerator = sum(left[token] * right[token] for token in shared)
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return numerator / (left_norm * right_norm)


def _query_vector(question: str) -> Dict[str, float]:
    tokens = _tokenize(question)
    counts: Dict[str, int] = {}
    for token in tokens:
        counts[token] = counts.get(token, 0) + 1
    length = max(len(tokens), 1)
    return {token: count / length for token, count in counts.items()}


def _clean_answer_text(chunk_text: str, title: str) -> str:
    cleaned = chunk_text.replace("\n", " ").strip()
    if cleaned.startswith("# "):
        cleaned = cleaned[2:].strip()
    title_prefix = title.strip()
    if cleaned.lower().startswith(title_prefix.lower()):
        cleaned = cleaned[len(title_prefix):].strip()
    return cleaned


def run_pipeline(question: str = DEFAULT_QUERY) -> Dict[str, object]:
    sample_info = build_sample_dataset()
    documents = _load_rows(sample_info["documents_path"])
    qa_rows = _load_rows(sample_info["qa_path"])
    chunks = _chunk_documents(documents)
    vectors = _build_tfidf(chunks)
    query_vector = _query_vector(question)

    scored = []
    for chunk, vector in zip(chunks, vectors):
        similarity = round(_cosine(query_vector, vector), 4)
        scored.append({**chunk, "similarity": similarity})
    scored.sort(key=lambda row: row["similarity"], reverse=True)
    top_chunks = scored[:3]

    top_chunk = top_chunks[0]
    answer = _clean_answer_text(str(top_chunk["chunk_text"]), str(top_chunk["title"]))

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    report_path = PROCESSED_DIR / "document_rag_databricks_report.json"
    report = {
        "dataset_source": sample_info["dataset_source"],
        "runtime_mode": "local_retrieval_fallback",
        "document_count": len(documents),
        "qa_count": len(qa_rows),
        "chunk_count": len(chunks),
        "query": question,
        "top_doc_id": top_chunk["doc_id"],
        "top_chunk_id": top_chunk["chunk_id"],
        "top_similarity": top_chunk["similarity"],
        "answer": answer,
        "top_chunks": top_chunks,
        "report_artifact": str(report_path),
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
