CREATE TABLE IF NOT EXISTS main.document_rag.silver_document_chunks (
  chunk_id STRING,
  doc_id STRING,
  title STRING,
  url STRING,
  chunk_order INT,
  chunk_text STRING
)
USING DELTA;
