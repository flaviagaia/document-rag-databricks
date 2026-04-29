# Databricks notebook source
# MAGIC %md
# MAGIC # Silver chunking
# MAGIC
# MAGIC Splits markdown/text documents into chunked rows with metadata ready for embedding.

# COMMAND ----------

catalog = "main"
schema = "document_rag"
silver_table = f"{catalog}.{schema}.silver_document_chunks"

print(f"Target silver table: {silver_table}")
