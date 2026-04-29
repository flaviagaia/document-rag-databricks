CREATE TABLE IF NOT EXISTS main.document_rag.gold_vector_index_source (
  chunk_id STRING,
  doc_id STRING,
  title STRING,
  url STRING,
  chunk_text STRING,
  embedding ARRAY<FLOAT>
)
USING DELTA;
