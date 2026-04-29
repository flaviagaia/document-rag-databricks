from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

from src.rag_pipeline import run_pipeline


class QueryRequest(BaseModel):
    question: str


app = FastAPI(
    title="Document RAG Databricks",
    description=(
        "Document RAG architecture inspired by the Databricks ecosystem, including bronze/silver/gold layers, "
        "Vector Search source tables, and a Streamlit-ready app path."
    ),
    version="1.0.0",
)


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/ask")
def ask(request: QueryRequest) -> dict[str, object]:
    return run_pipeline(request.question)
