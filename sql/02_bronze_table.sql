CREATE TABLE IF NOT EXISTS main.document_rag.bronze_documents (
  doc_id STRING,
  url STRING,
  title STRING,
  document STRING,
  md_document STRING,
  ingested_at TIMESTAMP
)
USING DELTA;
