# Databricks notebook source
# MAGIC %md
# MAGIC # Gold index source
# MAGIC
# MAGIC Produces the Delta table that acts as the source for Mosaic AI Vector Search.

# COMMAND ----------

catalog = "main"
schema = "document_rag"
gold_table = f"{catalog}.{schema}.gold_vector_index_source"

print(f"Target gold table: {gold_table}")
