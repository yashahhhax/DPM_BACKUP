# Databricks notebook source
# MAGIC %md ### Helper function to replace empty strings with NULL
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

def replace_empty_string_with_null(col):
        return F.when(F.col(col) == "", None).otherwise(F.col(col))
