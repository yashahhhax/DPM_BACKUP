# Databricks notebook source
dbutils.widgets.text("env","")
env = dbutils.widgets.get("env")

# COMMAND ----------

print(env)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
from pyspark.sql import SparkSession


# dataset details list of tuples
data = [
    ('travelbox', '672271f2-0575-4e10-837e-f8e9213986e6', '2c5d76c1-5167-4cf8-ae85-bbd13f71e8b1', 'TravelBox', '', 'Y')
    # ('maintenance', '672271f2-0575-4e10-837e-f8e9213986e6', 'da530df1-dc0a-4b18-ba52-7761b09b506d', 'Rides Performance', 'Ride', 'Y'),
    # ('maintenance', '672271f2-0575-4e10-837e-f8e9213986e6', '699ca4e3-44f3-4272-9ea1-1633a8ac4294', 'Work Order Analysis Report', 'Maximo', 'Y'),
    # ('maintenance', '672271f2-0575-4e10-837e-f8e9213986e6', '3131910c-0d8b-4ee2-a9a3-050ae0f1d3ab', 'Labour Hours Report', 'Maximo', 'Y'),
    # ('maintenance', '672271f2-0575-4e10-837e-f8e9213986e6', '0c3e471e-c65e-4ec0-bac4-2fbc8301ab66', 'Service Request Report', 'Maximo', 'Y'),
    # ('maintenance', '672271f2-0575-4e10-837e-f8e9213986e6', 'a6bb20f3-2cc5-40d4-a719-49143f7e5761', 'Breakdown Maintenance Analysis Report', 'Maximo', 'Y')
]

# Define schema
schema = StructType([
    StructField("usecase", StringType(), True),
    StructField("workspace_id", StringType(), True),
    StructField("dataset_id", StringType(), True),
    StructField("report_name", StringType(), True),
    StructField("module", StringType(), True),
    StructField("refresh_required", StringType(), True),
])

# Create DataFrame
df = spark.createDataFrame(data, schema)
display(df)


# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS transformation_{env}.config.pbi_datasets (
    usecase STRING,
    workspace_id STRING,
    dataset_id STRING,
    report_name STRING,
    module STRING,
    refresh_required STRING
)
USING DELTA
""")


# COMMAND ----------

# Define database and table name
table_name = f"transformation_{env}.config.pbi_datasets"

# Perform merge (upsert) using dataset_id
delta_table = DeltaTable.forName(spark, table_name)

(
    delta_table.alias("target")
    .merge(
        source=df.alias("source"),
        condition="target.dataset_id = source.dataset_id"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
