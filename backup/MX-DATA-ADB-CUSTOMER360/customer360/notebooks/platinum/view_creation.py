# Databricks notebook source
# dbutils.widgets.text("env", "dev")
# env = dbutils.widgets.get("env")
# dbutils.widgets.text("object_name", "vw_transactions")
# object_name = dbutils.widgets.get("object_name")
# dbutils.widgets.text("table_name", "transactions")

# COMMAND ----------

# DBTITLE 1,include view query
# MAGIC %run ../../config/view_queries

# COMMAND ----------

# DBTITLE 1,include base functions
# MAGIC %run ../../shared_code/common_functions/base_functions

# COMMAND ----------

# DBTITLE 1,initialize variables
try:
    notebook_vars = [
        'env','object_name','table_name'
    ]
    env,object_name,table_name= get_notebook_params(*notebook_vars, verbose=True)
except Exception as e:
    print(f"Error: {str(e)}")
    raise Exception(f"Error: {str(e)}")

# COMMAND ----------

# DBTITLE 1,include table config
# MAGIC %run ../../config/table_config

# COMMAND ----------

# DBTITLE 1,optimization
#Table optimization
try:
    if table_name in c360_table_config:
        print("Optimizing Table before view creation...")
        config = c360_table_config[table_name]

        target_table_ref = config["target_table"].format(env=env)
        print(f"Target Table Reference: {target_table_ref}")

        cluster_key = config["cluster_by"]
        print(f"Cluster By: {cluster_key}")

        existing_cluster_keys = spark.sql(f"DESCRIBE DETAIL {target_table_ref}").select("clusteringColumns").collect()[0][0]
        #existing cluster keys match with config then skip ALTER TABLE
        if existing_cluster_keys and set(existing_cluster_keys) == set([k.strip() for k in cluster_key.split(',')]):
            print("Cluster keys already match. Skipping ALTER TABLE.")
        else:
            spark.sql(f"ALTER TABLE {target_table_ref} CLUSTER BY ({cluster_key})")

        spark.sql(f"OPTIMIZE {target_table_ref}")
    else:
        print(f"Table '{table_name}' not found in c360_table_config. Skipping optimization.")
except Exception as e:
    print(f"Error optimizing table: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Parse object names
catalog = f"transformation_{env}"
schema = "plt_customer360"

object_names = [name.strip() for name in object_name.split(",") if name.strip()]
print(f"Objects to process: {object_names}")

# COMMAND ----------

# DBTITLE 1,Create views or tables
#view creation for multiple objects
for obj_name in object_names:
    try:
        print(f"Processing: {obj_name}")
        select_query = c360_view_queries[obj_name].format(catalog=catalog, schema=schema)
        create_view_or_table(catalog=catalog, schema=schema, object_name=obj_name, select_query=select_query)
        print(f"Successfully created: {obj_name}")
    except KeyError:
        print(f"WARNING: '{obj_name}' not found in c360_view_queries. Skipping.")
    except Exception as e:
        print(f"ERROR creating '{obj_name}': {str(e)}")
        raise
