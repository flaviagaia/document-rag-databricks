# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion
# MAGIC
# MAGIC Ingests enterprise documentation into a bronze Delta table.

# COMMAND ----------

catalog = "main"
schema = "document_rag"
bronze_table = f"{catalog}.{schema}.bronze_documents"

# The real notebook would load the Hugging Face / enterprise corpus and write it to Delta.
print(f"Target bronze table: {bronze_table}")
