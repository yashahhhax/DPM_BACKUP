# Databricks notebook source
# DBTITLE 1,get_notebook_params
def get_notebook_params(*param_names, verbose=False):
    """
    Safely fetches Databricks notebook parameters (widgets).
    Missing parameters will return None instead of raising errors.
    
    Args:
        *param_names: List of widget names to retrieve.
        verbose (bool): If True, print info for each parameter.
        
    Returns:
        A single value if one param is requested, else a list of values.
    """
    values = []

    for name in param_names:
        try:
            value = dbutils.widgets.get(name)
        except:
            value = None
            if verbose:
                print(f"[INFO] Widget '{name}' not found. Defaulting to default value.")
        if verbose:
            print(f"[INFO] {name} = {value}")
        values.append(value)

    return values[0] if len(values) == 1 else values

# COMMAND ----------

# DBTITLE 1,table_exists
def table_exists(table_name):
    try:
        catalog, schema, table = table_name.split(".")
        result = spark.sql(f"""
            SELECT 1
            FROM {catalog}.information_schema.tables
            WHERE table_schema = '{schema}'
              AND table_name = '{table}'
        """).first()
        return result is not None
    except Exception:
        return False

# COMMAND ----------

# DBTITLE 1,add_audit_cols
def add_audit_cols(df, source):
    return (
        df.withColumn(
            "PARAM_DATE_KEY",
            F.date_format(F.to_date(F.lit(start_date), "yyyy-MM-dd"), "yyyyMMdd").cast("bigint")
        )
        .withColumn("LOADED_AT", F.from_utc_timestamp(F.current_timestamp(), "UTC+4"))
        .withColumn("CREATED_BY", F.lit(current_user_value))
        .withColumn("MODIFIED_BY", F.lit(current_user_value))
    )

# COMMAND ----------

# DBTITLE 1,compare_schema_lenient
def compare_schema_lenient(actual_schema, expected_schema):
    def normalize(schema):
        return sorted([
            (f.name.lower(), str(f.dataType).lower())  # normalize name and type
            for f in schema.fields
        ])
    
    return normalize(actual_schema) == normalize(expected_schema)


# COMMAND ----------

# DBTITLE 1,clean_col
# Standardize column names
import re

def clean_col(col_name):
    if not isinstance(col_name, str):
        col_name = str(col_name)

    cleaned = re.sub(r"[.\s]+", "_", col_name)       # Replace dots/spaces with _
    cleaned = re.sub(r"[^\w]", "_", cleaned)         # Replace other special chars
    cleaned = re.sub(r"__+", "_", cleaned)           # Collapse multiple underscores
    cleaned = cleaned.strip("_")                     # Trim leading/trailing _
    return cleaned.upper()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
 
def clean_string_columns(df):
    string_cols = {c for c, dtype in df.dtypes if dtype == "string"}
 
    def clean_expr(c):
        e = F.col(c)
        # Normalize invisible Unicode spaces FIRST
        e = F.regexp_replace(e, u"[\u00A0\u200B\u200C\u200D\uFEFF]", " ")
        # Clean leading, trailing, and extra spaces in between words
        e = F.regexp_replace(F.trim(e), r"\s+", " ")
        # normalize all dash-like characters to standard hyphen (-)
        e = F.translate(e, "–—−‐-‒", "------")
        e = F.regexp_replace(e, r"\?s\b", "'s")
        # replace � or ? with -
        e = F.regexp_replace(e, "[�?]|Â€“", "-")
        # Replace values that are *only spaces* (after cleaning) with empty string ""
        e = F.when(e.rlike(r"^\s*$"), "").otherwise(e)
        return e.alias(c)
 
    select_exprs = [clean_expr(c) if c in string_cols else F.col(c) for c in df.columns]
    return df.select(*select_exprs)
    return df
 

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import re

def clean_and_return_df(source_table: str, skip_clean_string_columns: bool = False) -> DataFrame:

    """

    Cleans a Delta table and returns the cleaned DataFrame.
 
    Parameters:

    - source_table (str): Full name of the source table to read (e.g., db.schema.table)

    Returns:

    - DataFrame: Cleaned Spark DataFrame
 
    OPTIMIZED: dropna(how="all") now runs BEFORE dropDuplicates(). Dropping

    all-null rows doesn't change which rows survive deduplication (an

    all-null row collapses to one occurrence either way), but doing it first

    means the expensive shuffle inside dropDuplicates() has fewer rows to

    move. Final result is unchanged.

    """
 
    # 1. Read source table

    df = spark.table(source_table)
 
    columns_to_consider = [col for col in df.columns if col != 'adf_load_time']
 
    # 2. Drop rows with all nulls across considered columns (cheap filter,

    #    shrinks the dataset before the shuffle below)

    df = df.dropna(how="all", subset=columns_to_consider)
 
    # 3. Drop duplicate rows (excluding 'adf_load_time' if present)

    df = df.dropDuplicates(columns_to_consider)

    if not skip_clean_string_columns:

        # 4. Clean string columns

        df = clean_string_columns(df)
 
    return df
 

# COMMAND ----------

# DBTITLE 1,clean_string_columns
# from pyspark.sql import functions as F
# from pyspark.sql.types import StringType

# def clean_string_columns(df):
#     """
#     Cleans all string columns in the given DataFrame by:
#     1. Removing leading/trailing spaces
#     2. Replacing multiple spaces between words with a single space
#     Uses only Spark SQL functions (no UDFs).
#     """
    
#     for col_name, dtype in df.dtypes:
#         if dtype == "string":


#             # Normalize invisible Unicode spaces FIRST
#             df = df.withColumn(
#                 col_name,
#                 F.regexp_replace(
#                     F.col(col_name),
#                     u"[\u00A0\u200B\u200C\u200D\uFEFF]",
#                     " "
#                 )
#             )


#             # Clean leading, trailing, and extra spaces in between words
#             df = df.withColumn(col_name,F.regexp_replace(F.trim(F.col(col_name)), r"\s+", " "))

#             # normalize all dash-like characters to standard hyphen (-)
#             df = df.withColumn(
#                 col_name,
#                 F.translate(
#                     F.col(col_name),
#                     "–—−‐-‒",
#                     "------"
#                 )
#             )
#             df = df.withColumn(
#                 col_name,
#                 F.regexp_replace(F.col(col_name), r"\?s\b", "'s")
#             )
#             # replace � or ? with -
#             df = df.withColumn(col_name,F.regexp_replace(F.col(col_name), "[�?]|Â€“", "-"))
            
#             #Replace values that are *only spaces* (after cleaning) with empty string ""
#             df = df.withColumn(
#                 col_name,
#                 F.when(F.col(col_name).rlike(r"^\s*$"), "").otherwise(F.col(col_name))
#             )
#     return df

# COMMAND ----------

# DBTITLE 1,clean_and_return_df
# from pyspark.sql import DataFrame
# from pyspark.sql.functions import col
# import re
 
# def clean_and_return_df(source_table: str) -> DataFrame:
#     """
#     Cleans a Delta table and returns the cleaned DataFrame.
 
#     Parameters:
#     - source_table (str): Full name of the source table to read (e.g., db.schema.table)
#     Returns:
#     - DataFrame: Cleaned Spark DataFrame
#     """
 
#     # 1. Read source table
#     df = spark.table(source_table)
 
#     # 2. Drop duplicate rows (excluding 'adf_load_time' if present)
#     columns_to_consider = [col for col in df.columns if col != 'adf_load_time']
#     df = df.dropDuplicates(columns_to_consider)
 
#     # 3. Drop rows with all nulls across considered columns
#     df = df.dropna(how="all", subset=columns_to_consider)

#     # 4. Clean string columns
#     df = clean_string_columns(df)
 
#     return df

# COMMAND ----------

# DBTITLE 1,apply_schema_casting
from pyspark.sql.functions import col

def apply_schema_casting(df, schema_dict):
    """
    Casts columns of a DataFrame based on a provided schema dictionary.

    Parameters:
    - df (DataFrame): Input DataFrame to cast
    - schema_dict (dict): Dictionary with column names as keys and data types as values

    Returns:
    - DataFrame with casted columns
    """
    for col_name, col_type in schema_dict.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(col_type))
    return df


# COMMAND ----------

# DBTITLE 1,replace_empty_string_with_null
from pyspark.sql import functions as F
def replace_empty_string_with_null(col):
    # Remove common invisible chars: NBSP (\u00A0), zero-width space (\u200B)
    cleaned = F.regexp_replace(F.col(col), "[\u00A0\u200B]", "")
    # Trim spaces and check if empty
    return F.when(F.trim(cleaned) == "", None).otherwise(F.col(col))

# COMMAND ----------

# DBTITLE 1,log_gld_table_stats
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from datetime import date, datetime

def log_gld_table_stats(
    usecase: str,
    current_job_run_id: str,
    table_name: str,
    gold_stats: dict
    # load_type: str = None,
    # start_date=None,
    # end_date=None
):
    today = date.today()

    # Define the explicit schema to match the Delta table
    schema = StructType([
        StructField("stats_date", DateType(), True),
        StructField("current_job_run_id", StringType(), True),
        StructField("usecase", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("details", StructType([
            StructField("gold", StructType([
                StructField("gld_table", StringType(), True),
                StructField("dim_new_records", LongType(), True),
                StructField("dim_updated_records", IntegerType(), True),
                StructField("fact_inserted_records", LongType(), True),
                StructField("fact_updated_records", LongType(), True)
            ]), True)
        ]), True),
        StructField("logged_timestamp", TimestampType(), True)
    ])

    # Build the gold struct from provided dict
    gold_struct = {
        "gld_table": gold_stats.get("gld_table"),
        "dim_new_records": int(gold_stats.get("dim_new_records_count", 0)),
        "dim_updated_records": int(gold_stats.get("dim_updated_records_count", 0)),
        "fact_inserted_records": int(gold_stats.get("fact_inserted_records_count", 0)),
        "fact_updated_records": int(gold_stats.get("fact_updated_records_count", 0))
    }

    row_data = [{
        "stats_date": today,
        "current_job_run_id": current_job_run_id,
        "usecase": usecase,
        "table_name": table_name,
        "details": {
            "gold": gold_struct
        },
        "logged_timestamp": None
    }]

    df = spark.createDataFrame(row_data, schema=schema).withColumn("logged_timestamp", current_timestamp())

    df.withColumn("logged_timestamp", current_timestamp()) \
        .write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(f"transformation_{env}.etl_audit.adb_table_stats")

    display(df)

# COMMAND ----------

# DBTITLE 1,log_brz_table_stats
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from datetime import date, datetime

def log_brz_table_stats(
    usecase: str,
    current_job_run_id: str,
    table_name: str,
    gold_stats: dict
    # load_type: str = None,
    # start_date=None,
    # end_date=None
):
    today = date.today()

    # Define the explicit schema to match the Delta table
    schema = StructType([
        StructField("stats_date", DateType(), True),
        StructField("current_job_run_id", StringType(), True),
        StructField("usecase", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("details", StructType([
            StructField("gold", StructType([
                StructField("gld_table", StringType(), True),
                StructField("dim_new_records", LongType(), True),
                StructField("dim_updated_records", IntegerType(), True),
                StructField("fact_inserted_records", LongType(), True),
                StructField("fact_updated_records", LongType(), True)
            ]), True)
        ]), True),
        StructField("logged_timestamp", TimestampType(), True)
    ])

    # Build the gold struct from provided dict
    gold_struct = {
        "gld_table": gold_stats.get("gld_table"),
        "dim_new_records": int(gold_stats.get("dim_new_records_count", 0)),
        "dim_updated_records": int(gold_stats.get("dim_updated_records_count", 0)),
        "fact_inserted_records": int(gold_stats.get("fact_inserted_records_count", 0)),
        "fact_updated_records": int(gold_stats.get("fact_updated_records_count", 0))
    }

    row_data = [{
        "stats_date": today,
        "current_job_run_id": current_job_run_id,
        "usecase": usecase,
        "table_name": table_name,
        "details": {
            "gold": gold_struct
        },
        "logged_timestamp": None
    }]

    df = spark.createDataFrame(row_data, schema=schema).withColumn("logged_timestamp", current_timestamp())

    df.withColumn("logged_timestamp", current_timestamp()) \
        .write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(f"ingestion_{env}.etl_audit.adb_table_stats")

    display(df)

# COMMAND ----------

# DBTITLE 1,get_df_stats
from pyspark.sql import DataFrame

def get_df_stats(df: DataFrame) -> tuple[int, int]:
    return df.count(), len(df.columns)

# COMMAND ----------

# DBTITLE 1,get_default_expr
def get_default_expr(data_type):
    if data_type.startswith("STRING"):
        return "'Unknown'"
    elif data_type.startswith("BIGINT") or data_type.startswith("INT"):
        return "-99999"
    elif data_type.startswith("BOOLEAN"):
        return "NULL"
    elif data_type.startswith("DATE"):
        return "TIMESTAMP '1900-01-01'"
    elif data_type.startswith("TIMESTAMP"):
        return "TIMESTAMP '1900-01-01 00:00:00'"
    elif data_type.startswith("FLOAT") or data_type.startswith("DOUBLE") or data_type.startswith("DECIMAL"):
        return "-99999.0"
    else:
        return "NULL"
 

# COMMAND ----------

# DBTITLE 1,implement_scd_type2
def implement_scd_type2(source_table_ref, target_table_ref, surrogate_keys, primary_key_cols, scd_columns):
    """
    Implements SCD Type 2 on Delta tables in Databricks using PySpark.

    Parameters:
    - source_table_ref: Source table reference (catalog.schema.table)
    - target_table_ref: Target table reference (catalog.schema.table)
    - surrogate_keys: Surrogate key column name
    - primary_key_cols: Business key column names
    - scd_columns: List of columns to track changes

    Returns:
    - True if successful
    """
    try:
        # print(f"Starting SCD Type 2 processing for {target_table_ref}")

        # Reads the latest full snapshot of the source (Silver) table into a DataFrame
        source_df = spark.table(source_table_ref)

        # Generate hash value using sha2 for change detection
        # source_df = source_df.withColumn(
        #     "HASHCOL",
        #     F.sha2( F.concat_ws("|", *[F.trim(F.coalesce(F.col(c), F.lit("NULL"))).cast("string") for c in scd_columns]), 256)
        #     )
        
        source_df = source_df.withColumn(
            "HASHCOL",F.sha2(F.concat_ws("|",*[coalesce(col(c).cast("string"), lit("NULL"))for c in scd_columns]),256)
            )
       
        # Get the current user for audit columns like created_by, modified_by
        current_user_df = spark.sql("SELECT current_user() AS user")
        current_user_value = current_user_df.collect()[0]["user"]
        # print(f"Current User: {current_user_value}")

        # Fetch current timestamp from Spark (Sets timezone to Dubai)
        spark.conf.set("spark.sql.session.timeZone", "Asia/Dubai")
        current_ts_row = spark.sql("SELECT current_timestamp() AS ts").collect()[0]
        current_ts_str = current_ts_row["ts"]

        # Convert to required format: 'YYYY-MM-DDTHH:MM:SS.sss'
        current_ts = datetime.strftime(current_ts_str, "%Y-%m-%dT%H:%M:%S.%f")[:-3]  # Trim microseconds to milliseconds

        # Default eff_end_datetime for currently active records
        hypothetical_end_date = "9999-12-31T00:00:00.000"

        # print(f"Current Timestamp: {current_ts}")
        # print(f"Hypothetical End Timestamp: {hypothetical_end_date}")

        #Check if target table exists
        tables = spark.sql(f"SHOW TABLES IN {target_table_ref.split('.')[0]}.{target_table_ref.split('.')[1]}")
        target_exists = (tables.filter(F.col("tableName") == target_table_ref.split(".")[2]).count() > 0 )

        if target_exists:
            # print(f"Target table exists: {target_table_ref}")

            #Load the target table
            target_df = spark.table(target_table_ref)

            # Null-safe join condition
            join_condition = reduce(
                lambda x, y: x & y,
                [F.col(f"source.{col}").eqNullSafe(F.col(f"target.{col}")) for col in primary_key_cols]
            )

            # Identify only new rows (not found in target)
            new_records_df = (
                source_df.alias("source")
                .join(
                    target_df.filter(F.col("current_flag") == 'Y').alias("target"),
                    join_condition,
                    "left"
                )
                .filter(
                    F.col("target.hashcol").isNull()  # New record: target row not found
                )
                .select("source.*")
            )

            new_records_count = new_records_df.count()

            # Identify only updated rows (same primary key, different hashcol)
            updated_records_df = (
                source_df.alias("source")
                .join(
                    target_df.filter(F.col("current_flag") == 'Y').alias("target"),
                    join_condition,
                    "left"
                )
                .filter(
                    (F.col("target.hashcol").isNotNull()) & 
                    (F.col("target.hashcol") != F.col("source.hashcol"))  # Hash mismatch indicates update
                )
                .select("source.*")
            )

            updated_records_count = updated_records_df.count()

            # Union to get updated and new records
            updated_and_new_df = new_records_df.unionByName(updated_records_df)
            # display(updated_and_new_df)

            #Prepare for merge by creating temp view
            updated_and_new_df.createOrReplaceTempView("temp_updated_and_new_df")

            #Generate merge condition
            join_condition = " AND ".join([f"target.{col} <=> source.{col}" for col in primary_key_cols])
            
            try:
                spark.conf.set(
                    "spark.databricks.delta.schema.autoMerge.enabled", "true"
                )
            except Exception as e:
                print("Warning: Unable to enable schema auto-merge. Proceeding without it.")
                spark.conf.set("spark.sql.ansi.enabled", "false")


            # Remove any overlaps
            scd_columns = [col for col in scd_columns if col not in primary_key_cols]
            # print(f"Filtered SCD columns: {scd_columns}")
            
            # Handle empty other_columns in CREATE TABLE
            primary_columns_ddl = ", ".join([col for col in primary_key_cols])
            scd_columns_ddl = ", ".join([col for col in scd_columns])

            # Build source columns expressions
            source_columns_exprs = ", ".join([f"source.{col}" for col in primary_key_cols + scd_columns])

            # Build your final dynamic merge query
            merge_query = f"""
            MERGE INTO {target_table_ref} AS target
            USING temp_updated_and_new_df AS source
            ON {join_condition}

            WHEN MATCHED AND target.HASHCOL <> source.HASHCOL AND target.CURRENT_FLAG = 'Y' THEN
            UPDATE SET 
                target.CURRENT_FLAG = 'N',
                target.EFF_END_DATETIME = '{current_ts}',
                target.MODIFIED_DATETIME = '{current_ts}',
                target.MODIFIED_BY = '{current_user_value}'
            """

            # Step 2: Execute single MERGE statement
            # print(f"Running merge query:\n{merge_query}")
            spark.sql(merge_query)

            # Add all audit columns before writing
            updated_and_new_df = updated_and_new_df \
                .withColumn("CURRENT_FLAG", F.lit('Y')) \
                .withColumn("EFF_START_DATETIME", F.lit(current_ts).cast("timestamp")) \
                .withColumn("EFF_END_DATETIME", F.lit(hypothetical_end_date).cast("timestamp")) \
                .withColumn("CREATED_DATETIME", F.lit(current_ts).cast("timestamp")) \
                .withColumn("MODIFIED_DATETIME", F.lit(current_ts).cast("timestamp")) \
                .withColumn("CREATED_BY", F.lit(current_user_value)) \
                .withColumn("MODIFIED_BY", F.lit(current_user_value))

            updated_and_new_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table_ref)

            # print(f"SCD Type 2 processing completed successfully for {target_table_ref}")
            return True, new_records_count, updated_records_count

        else:
            # print(f"Target table does not exist (first-time load) for {target_table_ref}")

            # Count of new records = all records in source for first-time load
            new_records_count = source_df.count()
            
            # Register DataFrame as a temporary SQL view
            source_df.createOrReplaceTempView("temp_source_view")

            # Generate DDL for surrogate keys
            surrogate_keys_ddl = ",\n".join(
                f"{key} BIGINT GENERATED ALWAYS AS IDENTITY" for key in surrogate_keys
            )

            # print(surrogate_keys_ddl)
            # If the Target Table Doesn’t Exist → Create It First
            create_table_query = f"""
            CREATE TABLE {target_table_ref} (
                {surrogate_keys_ddl},
                {dim_schema_ddl},
                {dim_audit_columns_ddl}
            ) USING DELTA
            """
            spark.sql(create_table_query)
            # print(f"Target table created: {target_table_ref}")

            # Remove any overlaps
            scd_columns = [col for col in scd_columns if col not in primary_key_cols]
                        
            # Handle empty other_columns in CREATE TABLE
            primary_columns = ", ".join([col for col in primary_key_cols])
            scd_columns = ", ".join([col for col in scd_columns])

            default_values_expr = []

            # # Surrogate key(s)
            # for key in surrogate_keys:
            #     default_values_expr.append(-999999)

            # Dimension columns
            for col_def in dim_schema_ddl.split(","):
                col_name, col_type = col_def.strip().split(None, 1)
                default_values_expr.append(get_default_expr(col_type.upper()))

            # Audit columns
            for col_def in dim_audit_columns_ddl.split(","):
                parts = col_def.strip().split(None, 1)
                col_name = parts[0]
                col_type = parts[1].upper()

                if col_name == "HASHCOL":
                    if "STRING" in col_type:
                        default_values_expr.append("'Unknown'")
                    else:
                        default_values_expr.append("CAST(NULL AS " + col_type + ")")
                elif col_name == "CURRENT_FLAG":
                    default_values_expr.append("'Y'")
                elif col_name in ["EFF_START_DATETIME", "EFF_END_DATETIME", "CREATED_DATETIME", "MODIFIED_DATETIME"]:
                    default_values_expr.append("TIMESTAMP '1900-01-01 00:00:00'")
                elif col_name in ["CREATED_BY", "MODIFIED_BY"]:
                    default_values_expr.append("'Unknown'")
                else:
                    default_values_expr.append("NULL")

            default_values_expr = [str(value) for value in default_values_expr]

            # Construct the first-time load query dynamically
            default_load_query = f"""
            INSERT INTO {target_table_ref} ({primary_columns},
                {scd_columns}, HASHCOL, 
                CURRENT_FLAG, EFF_START_DATETIME, EFF_END_DATETIME, 
                CREATED_DATETIME, MODIFIED_DATETIME, CREATED_BY, MODIFIED_BY)
            VALUES ({", ".join(default_values_expr)})
            """
            spark.sql(default_load_query)

            # Construct the first-time load query dynamically
            first_load_query = f"""
            INSERT INTO {target_table_ref} ({primary_columns},
                {scd_columns}, HASHCOL, 
                        CURRENT_FLAG, EFF_START_DATETIME, EFF_END_DATETIME, 
                        CREATED_DATETIME, MODIFIED_DATETIME, CREATED_BY, MODIFIED_BY)
            SELECT 
                {primary_columns},
                {scd_columns},
                HASHCOL, 'Y', '{current_ts}', '{hypothetical_end_date}', 
                '{current_ts}', '{current_ts}', 
                '{current_user_value}', '{current_user_value}'
            FROM temp_source_view;
            """

            spark.sql(first_load_query)

            print(f"SCD Type 2 processing completed successfully for {target_table_ref}")
            return True, new_records_count, 0

    except Exception as e:
        error_message = f"Error processing SCD Type 2 for {target_table_ref}: {str(e)}"
        print(error_message)
        import traceback
        print(traceback.format_exc())
    
        # Explicitly raise an exception to fail the Databricks task/job/workflow
        raise RuntimeError(error_message)

# COMMAND ----------

# DBTITLE 1,implement_scd_type2_fact
def implement_scd_type2_fact(source_table_ref, target_table_ref, surrogate_keys, primary_key_cols, scd_columns, passthrough_columns=[]):
    """
    Implements SCD Type 2 on Delta tables in Databricks using PySpark.

    Parameters:
    - source_table_ref: Source table reference (catalog.schema.table)
    - target_table_ref: Target table reference (catalog.schema.table)
    - surrogate_keys: Surrogate key column name
    - primary_key_cols: Business key column names
    - scd_columns: List of columns to track changes
    - passthrough_columns: List of columns to propagate from source to target without SCD tracking

    Returns:
    - True if successful
    """
    try:
        source_df = spark.table(source_table_ref)

        source_df = source_df.withColumn(
            "HASHCOL",
            F.sha2(F.concat_ws("|", *[F.trim(F.coalesce(F.col(c), F.lit("NULL"))).cast("string") for c in scd_columns]), 256)
        )

        current_user_df = spark.sql("SELECT current_user() AS user")
        current_user_value = current_user_df.collect()[0]["user"]

        spark.conf.set("spark.sql.session.timeZone", "Asia/Dubai")
        current_ts_row = spark.sql("SELECT current_timestamp() AS ts").collect()[0]
        current_ts_str = current_ts_row["ts"]
        current_ts = datetime.strftime(current_ts_str, "%Y-%m-%dT%H:%M:%S.%f")[:-3]
        print(current_ts)

        hypothetical_end_date = "9999-12-31T00:00:00.000"

        tables = spark.sql(f"SHOW TABLES IN {target_table_ref.split('.')[0]}.{target_table_ref.split('.')[1]}")
        target_exists = (tables.filter(F.col("tableName") == target_table_ref.split(".")[2]).count() > 0)

        if target_exists:
            target_df = spark.table(target_table_ref)

            join_condition = reduce(
                lambda x, y: x & y,
                [F.col(f"source.{col}").eqNullSafe(F.col(f"target.{col}")) for col in primary_key_cols]
            )

            new_records_df = (
                source_df.alias("source")
                .join(
                    target_df.filter(F.col("current_flag") == 'Y').alias("target"),
                    join_condition,
                    "left"
                )
                .filter(F.col("target.hashcol").isNull())
                .select("source.*")
            )

            new_records_count = new_records_df.count()

            updated_records_df = (
                source_df.alias("source")
                .join(
                    target_df.filter(F.col("current_flag") == 'Y').alias("target"),
                    join_condition,
                    "left"
                )
                .filter(
                    (F.col("target.hashcol").isNotNull()) &
                    (F.col("target.hashcol") != F.col("source.hashcol"))
                )
                .select("source.*")
            )

            updated_records_count = updated_records_df.count()

            updated_and_new_df = new_records_df.unionByName(updated_records_df)
            updated_and_new_df.createOrReplaceTempView("temp_updated_and_new_df")

            join_condition = " AND ".join([f"target.{col} <=> source.{col}" for col in primary_key_cols])
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

            scd_columns = [col for col in scd_columns if col not in primary_key_cols]

            merge_query = f"""
            MERGE INTO {target_table_ref} AS target
            USING temp_updated_and_new_df AS source
            ON {join_condition}

            WHEN MATCHED AND target.HASHCOL <> source.HASHCOL AND target.CURRENT_FLAG = 'Y' THEN
            UPDATE SET 
                target.CURRENT_FLAG = 'N',
                target.EFF_END_DATETIME = '{current_ts}',
                target.MODIFIED_DATETIME = '{current_ts}',
                target.MODIFIED_BY = '{current_user_value}'
            """
            spark.sql(merge_query)

            updated_and_new_df = updated_and_new_df \
                .withColumn("CURRENT_FLAG", F.lit('Y')) \
                .withColumn("EFF_START_DATETIME", F.lit(current_ts).cast("timestamp")) \
                .withColumn("EFF_END_DATETIME", F.lit(hypothetical_end_date).cast("timestamp")) \
                .withColumn("CREATED_DATETIME", F.lit(current_ts).cast("timestamp")) \
                .withColumn("MODIFIED_DATETIME", F.lit(current_ts).cast("timestamp")) \
                .withColumn("CREATED_BY", F.lit(current_user_value)) \
                .withColumn("MODIFIED_BY", F.lit(current_user_value))

            updated_and_new_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table_ref)

            return True, new_records_count, updated_records_count

        else:
            new_records_count = source_df.count()
            source_df.createOrReplaceTempView("temp_source_view")

            surrogate_keys_ddl = ",\n".join(
                f"{key} BIGINT GENERATED ALWAYS AS IDENTITY" for key in surrogate_keys
            )

            create_table_query = f"""
            CREATE TABLE {target_table_ref} (
                {surrogate_keys_ddl},
                {fact_schema_ddl},
                {dim_audit_columns_ddl}
            ) USING DELTA
            """
            spark.sql(create_table_query)

            scd_columns = [col for col in scd_columns if col not in primary_key_cols]

            primary_columns = ", ".join([col for col in primary_key_cols])
            scd_columns_expr = ", ".join([col for col in scd_columns])
            passthrough_expr = ", ".join(passthrough_columns)

            default_values_expr = []

            for col_def in fact_schema_ddl.split(","):
                col_name, col_type = col_def.strip().split(None, 1)
                default_values_expr.append(get_default_expr(col_type.upper()))

            for col_def in dim_audit_columns_ddl.split(","):
                parts = col_def.strip().split(None, 1)
                col_name = parts[0]
                col_type = parts[1].upper()

                if col_name == "HASHCOL":
                    default_values_expr.append("'Unknown'" if "STRING" in col_type else f"CAST(NULL AS {col_type})")
                elif col_name == "CURRENT_FLAG":
                    default_values_expr.append("'Y'")
                elif col_name in ["EFF_START_DATETIME", "EFF_END_DATETIME", "CREATED_DATETIME", "MODIFIED_DATETIME"]:
                    default_values_expr.append("TIMESTAMP '1900-01-01 00:00:00'")
                elif col_name in ["CREATED_BY", "MODIFIED_BY"]:
                    default_values_expr.append("'Unknown'")
                else:
                    default_values_expr.append("NULL")

            default_values_expr = [str(value) for value in default_values_expr]

            all_cols = ", ".join([primary_columns, scd_columns_expr, passthrough_expr, "HASHCOL",
                                    "CURRENT_FLAG", "EFF_START_DATETIME", "EFF_END_DATETIME",
                                    "CREATED_DATETIME", "MODIFIED_DATETIME", "CREATED_BY", "MODIFIED_BY"])

            # Build list for SELECT clause with audit values
            select_expr_list = []

            if primary_columns:
                select_expr_list.extend([col.strip() for col in primary_columns.split(",")])
            if scd_columns_expr:
                select_expr_list.extend([col.strip() for col in scd_columns_expr.split(",")])
            if passthrough_expr:
                select_expr_list.extend([col.strip() for col in passthrough_expr.split(",")])

            # Add audit columns
            select_expr_list.extend([
                "HASHCOL",
                f"'Y' AS CURRENT_FLAG",
                f"CAST('{current_ts}' AS TIMESTAMP) AS EFF_START_DATETIME",
                f"CAST('{hypothetical_end_date}' AS TIMESTAMP) AS EFF_END_DATETIME",
                f"CAST('{current_ts}' AS TIMESTAMP) AS CREATED_DATETIME",
                f"CAST('{current_ts}' AS TIMESTAMP) AS MODIFIED_DATETIME",
                f"'{current_user_value}' AS CREATED_BY",
                f"'{current_user_value}' AS MODIFIED_BY"
            ])

            # Final SELECT & INSERT columns
            final_select_expr = ", ".join(select_expr_list)
            final_insert_cols = ", ".join([col for col in all_cols.split(",") if col.strip()])

            # Final query
            first_load_query = f"""
            INSERT INTO {target_table_ref} ({final_insert_cols})
            SELECT {final_select_expr}
            FROM temp_source_view;
            """
            print(f"query_output : {first_load_query}")
            spark.sql(first_load_query)

            print(f"SCD Type 2 processing completed successfully for {target_table_ref}")
            return True, new_records_count, 0

    except Exception as e:
        error_message = f"Error processing SCD Type 2 for {target_table_ref}: {str(e)}"
        print(error_message)
        import traceback
        print(traceback.format_exc())
        raise RuntimeError(error_message)


# COMMAND ----------

# DBTITLE 1,add_last_updated_tag
from datetime import datetime
from zoneinfo import ZoneInfo

def add_last_updated_tag(table_name: str,is_failure=False):
    """
    Sets the 'last_updated' tag on the given Unity Catalog table using the current time in Asia/Dubai timezone.

    Parameters:
    - table_name (str): Fully qualified name of the table (e.g., 'catalog.schema.table')
    """
    try:
        # Get current timestamp in Dubai timezone
        dt = datetime.now(ZoneInfo("Asia/Dubai"))
        timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "0"
        
        if is_failure:
            # Apply tag to table
            spark.sql(f"""
                ALTER TABLE {table_name}
                SET TAGS ('last_failure' = '{timestamp}')
            """)
            print(f"Tag 'last_failure' set on {table_name} with value {timestamp}")
        else:
            # Apply tag to table
            spark.sql(f"""
                ALTER TABLE {table_name}
                SET TAGS ('last_updated' = '{timestamp}')
            """)
            print(f"Tag 'last_updated' set on {table_name} with value {timestamp}")
    except Exception as e:
        print(f"Failed to set tag on {table_name}: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,get_workspace_client
from databricks.sdk import WorkspaceClient

# Utility functions
def get_workspace_client() -> WorkspaceClient:
    """Create and return a Databricks workspace client."""
    return WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,find_job
from typing import Optional, Dict

def find_job(workspace_client: WorkspaceClient, job_name: str) -> Optional[Dict]:
    """Find a Databricks job by its name."""
    return next((job for job in workspace_client.jobs.list() if job.settings.name == job_name), None)

# COMMAND ----------

# DBTITLE 1,get_job_details
def get_job_details(job_name: str) -> Dict:
    """
    Get details of a Databricks job by its name.
    
    Args:
        job_name: Name of the Databricks job.
        
    Returns:
        Dict: Job details including job_id and name.
        
    Raises:
        ValueError: If no job is found with the given name.
    """
    workspace_client = get_workspace_client()
    job = find_job(workspace_client, job_name)
 
    if not job:
        raise ValueError(f"No job found with name: {job_name}")
    
    return {
        "job_id": job.job_id,
        "job_name": job.settings.name
    }

# COMMAND ----------

# DBTITLE 1,trigger_databricks_job_by_name
def trigger_databricks_job_by_name(job_name: str) -> Dict:
    """
    Trigger a Databricks job by its name.
    
    Args:
        job_name: Name of the Databricks job to trigger.
        
    Returns:
        Dict: Response with job run ID and status.
    """
    job = get_job_details(job_name)
    workspace_client = get_workspace_client()
    run_response = workspace_client.jobs.run_now(job_id=job["job_id"])
    
    return {
        "run_respons": run_response,
        "run_id": run_response.run_id,
        "status": "RUNNING"
    }
 

# COMMAND ----------

# DBTITLE 1,align_to_schema
def align_to_schema(df, target_schema):
    df_cols_lower = {c.lower(): c for c in df.columns}
    aligned_cols = []
 
    for f in target_schema.fields:
        target_col = f.name
        df_col = df_cols_lower.get(target_col.lower())
 
        if df_col:
            # Quote the column name with backticks safely
            quoted_col = f"`{df_col}`"
            aligned_cols.append(expr(quoted_col).cast(f.dataType).alias(target_col))
        else:
            aligned_cols.append(lit(None).cast(f.dataType).alias(target_col))
 
    return aligned_cols

# COMMAND ----------

# DBTITLE 1,get_incremental_data_and_metadata
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit, current_date, date_add, when
from pyspark.sql.types import DateType
from pyspark.sql import DataFrame
from datetime import datetime, timezone
from functools import reduce

def get_incremental_data_and_metadata(table_mappings, catalog, metadata_table, initial_load ='Y', lookback_days=0):
    """
    Handles data loading in three modes: full, incremental, and rolling.
    Behavior:
      - If initial_load == 'y' → always performs full load.
      - If initial_load == 'n' → behavior depends on read_mode:
          * incremental → loads new data after last adf_load_time (if metadata exists)
          * rolling → loads last X days using lookback_days
          * full → loads entire table
    Returns:
        incremental_dfs (list of tuples): [(source_table_name_df, df), ...]
        metadata_df_result (DataFrame or None)
        batch_id (str)
    """

    incremental_dfs = []
    metadata_rows = []

    # Generate shared batch ID and timestamp
    batch_id = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    updated_timestamp = datetime.now(timezone.utc)

    print(f"Batch ID: {batch_id}")
    print(f"Updated Timestamp: {updated_timestamp}")

    # Check if metadata table exists
    metadata_exists = spark.catalog.tableExists(metadata_table)
    metadata_df = spark.table(metadata_table) if metadata_exists else None
    print(f"Metadata table exists: {metadata_exists}")

    # Loop through mappings
    for fact_table, schema, source_table_name, read_mode, filter_date_key in table_mappings:
        read_mode = read_mode.lower().strip()
        initial_load_flag = initial_load.lower().strip()

        # Determine SQL query
        if initial_load_flag == 'y':
            # Always full load for initial run
            query = f"""
                SELECT * 
                FROM {catalog}.{schema}.{source_table_name}
            """
            print(f"[INITIAL FULL LOAD] {source_table_name}")

        elif initial_load_flag == 'n':
            if read_mode == "incremental" and metadata_exists:
                # Check metadata existence
                has_metadata = (
                    metadata_df.filter(
                        (metadata_df.fact_table_name == fact_table) &
                        (metadata_df.source_table_name == source_table_name)
                    ).count() > 0
                )

                if has_metadata:
                    query = f"""
                        SELECT *
                        FROM {catalog}.{schema}.{source_table_name}
                        WHERE CAST(adf_load_time AS timestamp) > (
                            SELECT MAX(CAST(adf_load_time AS timestamp))
                            FROM {metadata_table}
                            WHERE fact_table_name = '{fact_table}'
                              AND source_table_name = '{source_table_name}'
                        )
                    """
                    print(f"[INCREMENTAL LOAD] {source_table_name}")
                else:
                    # No prior metadata → full load
                    query = f"""
                        SELECT * 
                        FROM {catalog}.{schema}.{source_table_name}
                    """
                    print(f"[NO METADATA FOUND - FULL LOAD] {source_table_name}")

            elif read_mode == "rolling":
                # Rolling window load
                query = f"""
                    SELECT *
                    FROM {catalog}.{schema}.{source_table_name}
                    WHERE to_date(to_timestamp({filter_date_key})) >= date_add(current_date(), -{lookback_days})
                """
                print(f"[ROLLING LOAD] {source_table_name} (Last {lookback_days} days)")

            elif read_mode == "full":
                # Explicit full read
                query = f"""
                    SELECT * 
                    FROM {catalog}.{schema}.{source_table_name}
                """
                print(f"[FULL LOAD] {source_table_name}")

            else:
                # Fallback full read
                query = f"""
                    SELECT * 
                    FROM {catalog}.{schema}.{source_table_name}
                """
                print(f"[DEFAULT FULL LOAD] {source_table_name}")

        else:
            raise ValueError(f"Invalid initial_load flag '{initial_load}'. Expected 'y' or 'n'.")

        # Execute query and build metadata
        try:
            df = spark.sql(query)
            incremental_dfs.append((f"{source_table_name}_df", df))

            if df.head(1) and "adf_load_time" in df.columns:
                is_valid_filter = filter_date_key is not None and str(filter_date_key).strip() != ""

                meta_row_df = (
                    df.selectExpr("CAST(max(adf_load_time) AS TIMESTAMP) as adf_load_time")
                    .withColumn("batch_id", lit(batch_id))
                    .withColumn("fact_table_name", lit(fact_table))
                    .withColumn("source_table_name", lit(source_table_name))
                    .withColumn("read_mode", lit(read_mode))
                    .withColumn("filter_date_key", lit(filter_date_key).cast(DateType()))
                    .withColumn(
                        "filter_date_from",
                        when(lit(is_valid_filter), date_add(current_date(), -lookback_days))
                        .otherwise(lit(None).cast(DateType()))
                    )
                    .withColumn(
                        "filter_date_to",
                        when(lit(is_valid_filter), current_date())
                        .otherwise(lit(None).cast(DateType()))
                    )
                    .withColumn("updated_timestamp", lit(updated_timestamp))
                    .select(
                        "batch_id",
                        "fact_table_name",
                        "source_table_name",
                        "adf_load_time",
                        "read_mode",
                        "filter_date_key",
                        "filter_date_from",
                        "filter_date_to",
                        "updated_timestamp"
                    )
                )

                metadata_rows.append(meta_row_df)
            else:
                print(f"[SKIPPED METADATA] {source_table_name}: No data or missing 'adf_load_time'")

        except Exception as e:
            raise RuntimeError(f"Error reading table {catalog}.{schema}.{source_table_name}: {e}") from e

    # Combine metadata
    metadata_df_result = (
        reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), metadata_rows)
        if metadata_rows else None
    )

    return incremental_dfs, metadata_df_result, batch_id


# COMMAND ----------

# DBTITLE 1,replace_empty_string_with_null
from pyspark.sql import functions as F
def replace_empty_string_with_null(col):
    # Remove common invisible chars: NBSP (\u00A0), zero-width space (\u200B)
    cleaned = F.regexp_replace(F.col(col), "[\u00A0\u200B]", "")
    # Trim spaces and check if empty
    return F.when(F.trim(cleaned) == "", None).otherwise(F.col(col))

# COMMAND ----------

# DBTITLE 1,create_temp_view_from_query
def create_temp_view_from_query(final_view_name: str, query_str: str):
    """
    Creates or replaces a temp view using the given SQL query.

    Args:
        final_view_name (str): The name of the final temp view to create.
        query_str (str): The SQL query string to run.
    """
    sql_stmt = f"""
    CREATE OR REPLACE TEMP VIEW {final_view_name} AS
    {query_str}
    """
    spark.sql(sql_stmt)
    print(f"Created final temp view: {final_view_name}")

# COMMAND ----------

# DBTITLE 1,send_email_with_attachment
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd
import os

def send_email_with_attachment(sender_email, sender_password, receiver_email, subject, body_text, dataframe, file_name):
    try:
        # Use a writable path - current directory or a safe path
        file_path = os.path.join(os.getcwd(), file_name)
        
        # Save DataFrame to CSV
        dataframe.to_csv(file_path, index=False)

        # Compose email
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject

        # Attach body text (HTML format supported)
        msg.attach(MIMEText(body_text, 'html'))

        # Attach CSV file
        with open(file_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{file_name}"')
            msg.attach(part)

        # Send email via Outlook SMTP
        with smtplib.SMTP("smtp-mail.outlook.com", 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())

        print("✅ Email with CSV attachment sent successfully!")

        # Clean up file afterward
        os.remove(file_path)

    except Exception as e:
        print(f"❌ Error sending email: {e}")


# COMMAND ----------

# Dependency registration function
from typing import List, Dict

def create_workflow_dependencies_table(transformation_catalog: str) -> None:
    """
    Creates schema and workflow_dependencies table if they do not exist.
    """

    # Create schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {transformation_catalog}.config")

    # Create table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {transformation_catalog}.config.workflow_dependencies (
            use_case STRING COMMENT 'Business domain this workflow belongs to (e.g. drr, customer360). Used together with workflow_name to scope dependency inserts and deletes.',
            workflow_name STRING COMMENT 'Exact name of the Databricks job to trigger. Must match the job name in the Jobs API.',
            table_name STRING COMMENT 'Upstream table (schema.table) this workflow depends on. Orchestrator checks whether this table has been refreshed before triggering the workflow.',
            freshness_requirement_hours INT COMMENT 'Maximum allowed staleness of the upstream table in hours. When 0 (default), the table just needs any update after the workflow last run. When greater than 0, the table must have been updated within the last N hours, even if the workflow has not run in a long time. Prevents triggering on stale data.',
            hard_dependency BOOLEAN COMMENT 'When true, the upstream table (table_name) load must have completed successfully for the dependency to be satisfied. When false, the orchestrator only checks whether the table load was attempted — success or failure does not matter.',
            workflow_active_flag BOOLEAN COMMENT 'Indicates whether the workflow (workflow_name) is active. When true, the orchestrator will evaluate and trigger this workflow. When false, the orchestrator skips this workflow entirely.'
        )
    """)

def update_workflow_dependencies(
    transformation_catalog: str,
    use_case: str,
    workflow_name: str,
    dependencies: List[Dict[str, any]],
    workflow_active_flag: bool = True,
) -> None:
    """
    Update workflow dependencies in the config table.

    Args:
        transformation_catalog: Name of the transformation catalog.
        use_case: Use case string.
        workflow_name: Name of the workflow.
        dependencies: List of dictionaries, each describing a dependency with keys:
            - table_name (str)
            - freshness_requirement_hours (int, optional)
            - hard_dependency (bool, optional)
        workflow_active_flag (bool, optional): Whether the workflow is active. Defaults to True.

    Raises:
        TypeError: If dependencies is not a list of dicts.
    """

    # Fetch existing workflows to be able to validate input
    from databricks.sdk import WorkspaceClient
    workspace_client = WorkspaceClient()
    existing_workflows = [job.settings.name for job in workspace_client.jobs.list()]

    # Validate dependencies input
    if not isinstance(dependencies, list) or not all(isinstance(d, dict) for d in dependencies):
        raise TypeError("Expected dependencies to be a list of dictionaries.")

    # Unfortunately, we cannot fail on workflow existence and table existence
    # as deployment might be asynchronous (orchestrator deployed before usecase)
    if workflow_name not in existing_workflows:
        print(f"Workflow does not exist, {workflow_name} not in {existing_workflows}.")

    for dependency in dependencies:
        table_name = dependency['table_name']
        if not spark.catalog.tableExists(table_name):
            print(f"Table does not exist {table_name}, specified in dependency for {workflow_name}.")

    # Delete existing dependencies for workflow
    ##query = f"""
    ##DELETE FROM {transformation_catalog}.config.workflow_dependencies WHERE workflow_name = '{workflow_name}';
    ##"""
    ##spark.sql(query)

    # Delete the above block once the testing is completed
    query = f"""
    DELETE FROM {transformation_catalog}.config.workflow_dependencies WHERE workflow_name = '{workflow_name}' AND use_case = '{use_case}';
    """
    spark.sql(query)

    # Insert new dependencies if any provided
    if dependencies:
        values = ", ".join([
            f"('{use_case}', '{workflow_name}', '{d['table_name']}', "
            f"{d.get('freshness_requirement_hours', 0)}, {d.get('hard_dependency', True)}, {workflow_active_flag})"
            for d in dependencies
        ])
        query = f"""
        INSERT INTO {transformation_catalog}.config.workflow_dependencies VALUES {values};
        """
        spark.sql(query)

# COMMAND ----------

# DBTITLE 1,has_function
################################################################# Customer360 functions #################################################################
from pyspark.sql import functions as F
def hash_column(column_name):
    """
    Returns a SQL expression that trims, lowercases, and hashes the specified column using SHA-256.
    """
    return f"SHA2(lower(trim({column_name})), 256)"

# COMMAND ----------

# DBTITLE 1,email_standardization
def email_standardization(column_name):
    """
    Standardizes the email column by trimming whitespace, converting to lowercase, and validating the presence of '@'. Returns the cleaned email or NULL if invalid.
    """
    return f"IFF(lower(trim({column_name})) like '%@%', lower(trim({column_name})) ,NULL)"
    

# COMMAND ----------

# DBTITLE 1,yad_id_standardization
def yas_id_standardization(column_name):
    """
    Standardizes the 'yas_id' column by trimming whitespace, converting to lowercase, and validating the format (at least 35 characters and contains four hyphens). Returns the cleaned yad_id or NULL if invalid.
    """
    return f"IFF(length({column_name})>=35 and {column_name} like '%-%-%-%-%',lower(trim({column_name})),NULL)"

# COMMAND ----------

# DBTITLE 1,mobile_number_standardization
def master_mobile_number_standardization(df,column_name):
    """
    Standardizes the 'mobilephone' column in the input DataFrame.
    - Removes non-numeric characters.
    - Handles country code normalization (especially for UAE and other common codes).
    - Flags invalid numbers (e.g., repeated digits, incorrect lengths).
    - Extracts standardized country code and cleaned mobile number into new columns:
      'mobilephone_v1', 'mobilephone_v2', 'mobile_country_code', and 'mobile_num_cleaned'.
    """    
    df.createOrReplaceTempView("mobile_number_standardization")
    df = spark.sql(f"""
        select *   
            ,regexp_replace({column_name}, '[^0-9]', '') as mobilephone_v1
            ,CASE
                -- if all digits are same then invalid number example : 222222222,333333333
                WHEN mobilephone_v1 RLIKE r'^(\\\\d)\\\\1+$' THEN NULL
                --make by default 971 number
                WHEN mobilephone_v1 rlike r'^0[1-9]' and (country_code = 'AE' or nationality_code = 'AE') and length(mobilephone_v1) = 10 THEN concat('971',substr(mobilephone_v1,2))
                WHEN mobilephone_v1 not like '0%' and (country_code = 'AE' or nationality_code = 'AE')  and length(mobilephone_v1) = 9 THEN concat('971',mobilephone_v1)
                --duplicated 971
                WHEN mobilephone_v1 like '9719710%' THEN regexp_replace(mobilephone_v1,r'^9719710','971')
                WHEN mobilephone_v1 like '971971971%' THEN regexp_replace(mobilephone_v1,r'^971971971','971')
                WHEN mobilephone_v1 like '971971%' THEN regexp_replace(mobilephone_v1,r'^971971','971')
                --9710% like pattern 
                WHEN mobilephone_v1 like '971009710%' THEN regexp_replace(mobilephone_v1,r'^971009710','971')
                WHEN mobilephone_v1 like '97100971%' THEN regexp_replace(mobilephone_v1,r'^97100971','971')
                WHEN mobilephone_v1 like '9710971%' THEN regexp_replace(mobilephone_v1,r'^9710971','971')
                WHEN mobilephone_v1 like '9710%' and length(mobilephone_v1)  = 13 THEN concat('971',(substring(mobilephone_v1,5)))
                -- replace all zeros
                WHEN mobilephone_v1 rlike r'00+' THEN regexp_replace(mobilephone_v1,r'^00+','')
                --mobile number length more than 12 or less than 8 is invalid
                WHEN length(mobilephone_v1) < 8 or length(mobilephone_v1) > 12 or mobilephone_v1 = '' THEN null
                ELSE mobilephone_v1
            END AS mobilephone_v2
            -- COUNTRY CODE EXTRACTION
            ,CASE
                -- invalid cases
                WHEN mobilephone_v2 IS NULL THEN NULL
                WHEN length(mobilephone_v2) > 13 OR length(mobilephone_v2) < 8 THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^11[0-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^12[0-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^21[0-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^28[0-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^52[0-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^80[1-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^87[1-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^88[1-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^979' THEN NULL

                -- =========================
                -- 3 DIGIT COUNTRY CODES
                -- =========================
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^971\\d+' THEN '971'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^966\\d+' THEN '966'
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^965\\d+' THEN '965'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^972\\d+' THEN '972'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^353\\d+' THEN '353'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^351\\d+' THEN '351'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^420\\d+' THEN '420'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^380\\d+' THEN '380'
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^852\\d+' THEN '852'
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^961\\d+' THEN '961'
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^372\\d+' THEN '372'

                -- =========================
                -- 2 DIGIT COUNTRY CODES
                -- =========================
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^91\\d+' THEN '91'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^92\\d+' THEN '92'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^98\\d+' THEN '98'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^81\\d+' THEN '81'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^82\\d+' THEN '82'
                WHEN length(mobilephone_v2) = 13 AND mobilephone_v2 RLIKE r'^86\\d+' THEN '86'
                WHEN length(mobilephone_v2) = 10 AND mobilephone_v2 RLIKE r'^65\\d+' THEN '65'
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^61\\d+' THEN '61'
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^62\\d+' THEN '62'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^63\\d+' THEN '63'
                WHEN length(mobilephone_v2) = 13 AND mobilephone_v2 RLIKE r'^55\\d+' THEN '55'
                WHEN length(mobilephone_v2) = 13 AND mobilephone_v2 RLIKE r'^52\\d+' THEN '52'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^49\\d+' THEN '49'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^44\\d+' THEN '44'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^43\\d+' THEN '43'
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^41\\d+' THEN '41'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^39\\d+' THEN '39'
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^33\\d+' THEN '33'
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^34\\d+' THEN '34'
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^31\\d+' THEN '31'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^54\\d+' THEN '54'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^57\\d+' THEN '57'
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^20\\d+' THEN '20'
                WHEN length(mobilephone_v2) = 10 AND mobilephone_v2 RLIKE r'^45\\d+' THEN '45'
                WHEN length(mobilephone_v2) = 10 AND mobilephone_v2 RLIKE r'^47\\d+' THEN '47'
                WHEN length(mobilephone_v2) = 10 AND mobilephone_v2 RLIKE r'^354\\d+' THEN '354'
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^64\\d+' THEN '64'

                -- =========================
                -- 1 DIGIT COUNTRY CODES
                -- =========================
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^7\\d+' THEN '7'
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^1\\d+' THEN '1'
                
                ELSE NULL
            END AS mobile_country_code
            -- CLEANED MOBILE NUMBER EXTRACTION
            ,CASE
                -- invalid cases
                WHEN mobilephone_v2 IS NULL THEN NULL
                WHEN length(mobilephone_v2) > 13 OR length(mobilephone_v2) < 8 THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^11[0-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^12[0-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^21[0-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^28[0-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^52[0-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^80[1-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^87[1-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^88[1-9]' THEN NULL
                WHEN length(mobilephone_v2) > 11 AND mobilephone_v2 RLIKE r'^979' THEN NULL

                -- =========================
                -- 3 DIGIT COUNTRY CODES
                -- =========================
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^971\\d+' THEN regexp_replace(mobilephone_v2, '^971', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^966\\d+' THEN regexp_replace(mobilephone_v2, '^966', '')
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^965\\d+' THEN regexp_replace(mobilephone_v2, '^965', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^972\\d+' THEN regexp_replace(mobilephone_v2, '^972', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^353\\d+' THEN regexp_replace(mobilephone_v2, '^353', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^351\\d+' THEN regexp_replace(mobilephone_v2, '^351', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^420\\d+' THEN regexp_replace(mobilephone_v2, '^420', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^380\\d+' THEN regexp_replace(mobilephone_v2, '^380', '')
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^852\\d+' THEN regexp_replace(mobilephone_v2, '^852', '')
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^961\\d+' THEN regexp_replace(mobilephone_v2, '^961', '')
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^372\\d+' THEN regexp_replace(mobilephone_v2, '^372', '')

                -- =========================
                -- 2 DIGIT COUNTRY CODES
                -- =========================
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^91\\d+' THEN regexp_replace(mobilephone_v2, '^91', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^92\\d+' THEN regexp_replace(mobilephone_v2, '^92', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^98\\d+' THEN regexp_replace(mobilephone_v2, '^98', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^81\\d+' THEN regexp_replace(mobilephone_v2, '^81', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^82\\d+' THEN regexp_replace(mobilephone_v2, '^82', '')
                WHEN length(mobilephone_v2) = 13 AND mobilephone_v2 RLIKE r'^86\\d+' THEN regexp_replace(mobilephone_v2, '^86', '')
                WHEN length(mobilephone_v2) = 10 AND mobilephone_v2 RLIKE r'^65\\d+' THEN regexp_replace(mobilephone_v2, '^65', '')
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^61\\d+' THEN regexp_replace(mobilephone_v2, '^61', '')
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^62\\d+' THEN regexp_replace(mobilephone_v2, '^62', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^63\\d+' THEN regexp_replace(mobilephone_v2, '^63', '')
                WHEN length(mobilephone_v2) = 13 AND mobilephone_v2 RLIKE r'^55\\d+' THEN regexp_replace(mobilephone_v2, '^55', '')
                WHEN length(mobilephone_v2) = 13 AND mobilephone_v2 RLIKE r'^52\\d+' THEN regexp_replace(mobilephone_v2, '^52', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^49\\d+' THEN regexp_replace(mobilephone_v2, '^49', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^44\\d+' THEN regexp_replace(mobilephone_v2, '^44', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^43\\d+' THEN regexp_replace(mobilephone_v2, '^43', '')
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^41\\d+' THEN regexp_replace(mobilephone_v2, '^41', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^39\\d+' THEN regexp_replace(mobilephone_v2, '^39', '')
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^33\\d+' THEN regexp_replace(mobilephone_v2, '^33', '')
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^34\\d+' THEN regexp_replace(mobilephone_v2, '^34', '')
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^31\\d+' THEN regexp_replace(mobilephone_v2, '^31', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^54\\d+' THEN regexp_replace(mobilephone_v2, '^54', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^57\\d+' THEN regexp_replace(mobilephone_v2, '^57', '')
                WHEN length(mobilephone_v2) = 12 AND mobilephone_v2 RLIKE r'^20\\d+' THEN regexp_replace(mobilephone_v2, '^20', '')
                WHEN length(mobilephone_v2) = 10 AND mobilephone_v2 RLIKE r'^45\\d+' THEN regexp_replace(mobilephone_v2, '^45', '')
                WHEN length(mobilephone_v2) = 10 AND mobilephone_v2 RLIKE r'^47\\d+' THEN regexp_replace(mobilephone_v2, '^47', '')
                WHEN length(mobilephone_v2) = 10 AND mobilephone_v2 RLIKE r'^354\\d+' THEN regexp_replace(mobilephone_v2, '^354', '')
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^64\\d+' THEN regexp_replace(mobilephone_v2, '^64', '')

                -- =========================
                -- 1 DIGIT COUNTRY CODES
                -- =========================
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^7\\d+' THEN regexp_replace(mobilephone_v2, '^7', '')
                WHEN length(mobilephone_v2) = 11 AND mobilephone_v2 RLIKE r'^1\\d+' THEN regexp_replace(mobilephone_v2, '^1', '')

                -- general cases
                WHEN length(mobilephone_v2) >= 10 AND mobilephone_v2 LIKE '0%' THEN substring(mobilephone_v2, 2)
                WHEN length(mobilephone_v2) IN (8,9,10) THEN mobilephone_v2
                ELSE NULL
                END AS mobile_num_cleaned
            from mobile_number_standardization
        """)
    #drop unwanted columns
    df = df.drop("mobilephone_v1","mobilephone_v2")
    return df

# COMMAND ----------

def general_mobile_number_standardization(df,column_name):
    """
    This function standardizes the 'mobilephone' column in the input DataFrame.
    It creates a new column 'mobile_num_cleaned' with cleaned mobile numbers.
    """
    df.createOrReplaceTempView("mobile_number_standardization")
    df = spark.sql(f"""
        SELECT *
            ,regexp_replace({column_name}, '[^0-9]', '') as mobilephone_v1
            ,CASE WHEN mobilephone_v1 IS NULL THEN NULL
                WHEN length(mobilephone_v1) > 13 OR length(mobilephone_v1) < 8 THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^11[0-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^12[0-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^21[0-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^28[0-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^52[0-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^80[1-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^87[1-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^88[1-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^979' THEN NULL

                -- 3 DIGIT COUNTRY CODES
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^971\\d+' THEN '971'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^966\\d+' THEN '966'
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^965\\d+' THEN '965'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^972\\d+' THEN '972'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^353\\d+' THEN '353'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^351\\d+' THEN '351'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^420\\d+' THEN '420'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^380\\d+' THEN '380'
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^852\\d+' THEN '852'
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^961\\d+' THEN '961'
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^372\\d+' THEN '372'

                -- 2 DIGIT COUNTRY CODES
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^91\\d+' THEN '91'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^92\\d+' THEN '92'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^98\\d+' THEN '98'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^81\\d+' THEN '81'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^82\\d+' THEN '82'
                WHEN length(mobilephone_v1) = 13 AND mobilephone_v1 RLIKE r'^86\\d+' THEN '86'
                WHEN length(mobilephone_v1) = 10 AND mobilephone_v1 RLIKE r'^65\\d+' THEN '65'
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^61\\d+' THEN '61'
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^62\\d+' THEN '62'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^63\\d+' THEN '63'
                WHEN length(mobilephone_v1) = 13 AND mobilephone_v1 RLIKE r'^55\\d+' THEN '55'
                WHEN length(mobilephone_v1) = 13 AND mobilephone_v1 RLIKE r'^52\\d+' THEN '52'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^49\\d+' THEN '49'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^44\\d+' THEN '44'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^43\\d+' THEN '43'
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^41\\d+' THEN '41'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^39\\d+' THEN '39'
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^33\\d+' THEN '33'
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^34\\d+' THEN '34'
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^31\\d+' THEN '31'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^54\\d+' THEN '54'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^57\\d+' THEN '57'
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^20\\d+' THEN '20'
                WHEN length(mobilephone_v1) = 10 AND mobilephone_v1 RLIKE r'^45\\d+' THEN '45'
                WHEN length(mobilephone_v1) = 10 AND mobilephone_v1 RLIKE r'^47\\d+' THEN '47'
                WHEN length(mobilephone_v1) = 10 AND mobilephone_v1 RLIKE r'^354\\d+' THEN '354'
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^64\\d+' THEN '64'

                -- 1 DIGIT COUNTRY CODES
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^7\\d+' THEN '7'
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^1\\d+' THEN '1'
                ELSE NULL
            END AS {column_name}_country_code
            ,CASE
                -- invalid cases
                WHEN mobilephone_v1 IS NULL THEN NULL
                WHEN length(mobilephone_v1) > 13 OR length(mobilephone_v1) < 8 THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^11[0-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^12[0-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^21[0-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^28[0-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^52[0-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^80[1-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^87[1-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^88[1-9]' THEN NULL
                WHEN length(mobilephone_v1) > 11 AND mobilephone_v1 RLIKE r'^979' THEN NULL

                -- 3 DIGIT COUNTRY CODES
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^971\\d+' THEN regexp_replace(mobilephone_v1, '^971', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^966\\d+' THEN regexp_replace(mobilephone_v1, '^966', '')
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^965\\d+' THEN regexp_replace(mobilephone_v1, '^965', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^972\\d+' THEN regexp_replace(mobilephone_v1, '^972', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^353\\d+' THEN regexp_replace(mobilephone_v1, '^353', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^351\\d+' THEN regexp_replace(mobilephone_v1, '^351', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^420\\d+' THEN regexp_replace(mobilephone_v1, '^420', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^380\\d+' THEN regexp_replace(mobilephone_v1, '^380', '')
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^852\\d+' THEN regexp_replace(mobilephone_v1, '^852', '')
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^961\\d+' THEN regexp_replace(mobilephone_v1, '^961', '')
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^372\\d+' THEN regexp_replace(mobilephone_v1, '^372', '')

                -- 2 DIGIT COUNTRY CODES
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^91\\d+' THEN regexp_replace(mobilephone_v1, '^91', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^92\\d+' THEN regexp_replace(mobilephone_v1, '^92', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^98\\d+' THEN regexp_replace(mobilephone_v1, '^98', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^81\\d+' THEN regexp_replace(mobilephone_v1, '^81', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^82\\d+' THEN regexp_replace(mobilephone_v1, '^82', '')
                WHEN length(mobilephone_v1) = 13 AND mobilephone_v1 RLIKE r'^86\\d+' THEN regexp_replace(mobilephone_v1, '^86', '')
                WHEN length(mobilephone_v1) = 10 AND mobilephone_v1 RLIKE r'^65\\d+' THEN regexp_replace(mobilephone_v1, '^65', '')
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^61\\d+' THEN regexp_replace(mobilephone_v1, '^61', '')
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^62\\d+' THEN regexp_replace(mobilephone_v1, '^62', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^63\\d+' THEN regexp_replace(mobilephone_v1, '^63', '')
                WHEN length(mobilephone_v1) = 13 AND mobilephone_v1 RLIKE r'^55\\d+' THEN regexp_replace(mobilephone_v1, '^55', '')
                WHEN length(mobilephone_v1) = 13 AND mobilephone_v1 RLIKE r'^52\\d+' THEN regexp_replace(mobilephone_v1, '^52', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^49\\d+' THEN regexp_replace(mobilephone_v1, '^49', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^44\\d+' THEN regexp_replace(mobilephone_v1, '^44', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^43\\d+' THEN regexp_replace(mobilephone_v1, '^43', '')
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^41\\d+' THEN regexp_replace(mobilephone_v1, '^41', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^39\\d+' THEN regexp_replace(mobilephone_v1, '^39', '')
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^33\\d+' THEN regexp_replace(mobilephone_v1, '^33', '')
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^34\\d+' THEN regexp_replace(mobilephone_v1, '^34', '')
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^31\\d+' THEN regexp_replace(mobilephone_v1, '^31', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^54\\d+' THEN regexp_replace(mobilephone_v1, '^54', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^57\\d+' THEN regexp_replace(mobilephone_v1, '^57', '')
                WHEN length(mobilephone_v1) = 12 AND mobilephone_v1 RLIKE r'^20\\d+' THEN regexp_replace(mobilephone_v1, '^20', '')
                WHEN length(mobilephone_v1) = 10 AND mobilephone_v1 RLIKE r'^45\\d+' THEN regexp_replace(mobilephone_v1, '^45', '')
                WHEN length(mobilephone_v1) = 10 AND mobilephone_v1 RLIKE r'^47\\d+' THEN regexp_replace(mobilephone_v1, '^47', '')
                WHEN length(mobilephone_v1) = 10 AND mobilephone_v1 RLIKE r'^354\\d+' THEN regexp_replace(mobilephone_v1, '^354', '')
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^64\\d+' THEN regexp_replace(mobilephone_v1, '^64', '')

                -- 1 DIGIT COUNTRY CODES
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^7\\d+' THEN regexp_replace(mobilephone_v1, '^7', '')
                WHEN length(mobilephone_v1) = 11 AND mobilephone_v1 RLIKE r'^1\\d+' THEN regexp_replace(mobilephone_v1, '^1', '')

                -- general cases
                WHEN length(mobilephone_v1) >= 10 AND mobilephone_v1 LIKE '0%' THEN substring(mobilephone_v1, 2)
                WHEN length(mobilephone_v1) IN (8,9,10) THEN mobilephone_v1
                ELSE NULL
            END AS {column_name}_cleaned
        FROM mobile_number_standardization
    """)
    df = df.drop("mobilephone_v1")
    return df

# COMMAND ----------

# DBTITLE 1,gender_standardization
def gender_standardization(df,column_name):
    """
    This function standardizes the 'gender' column in the input DataFrame.
    It creates a new column 'gender_cleaned' with values:
      'F' for female/f, 'M' for male/m, 'Others' for others, and None for unrecognized values.
    """
    df = df.withColumn(
        "gender_cleaned",
        F.when(F.lower(F.trim(F.col(column_name))).isin("female", "f"), "F")
        .when(F.lower(F.trim(F.col(column_name))).isin("male", "m"), "M")
        .when(F.lower(F.trim(F.col(column_name))) == "others", "Others")
        .otherwise(None)
    )
    return df

# COMMAND ----------

# DBTITLE 1,dob_standardization
def dob_standardization(df,column_name):
    """
    Standardizes the 'birthdate' column in the input DataFrame by parsing various date formats
    and outputs a new column 'dob' in 'yyyy-MM-dd' format. Handles multiple delimiters, 
    compact forms, and month name variants.
    """
    df.createOrReplaceTempView('dob_standardization')
    df = spark.sql(f"""
            select * 
                ,date_format(
                    coalesce(
                        -- slash, year-first
                        try_to_date(trim({column_name}), 'yyyy/MM/dd'),
                        try_to_date(trim({column_name}), 'yyyy/MM/d'),
                        try_to_date(trim({column_name}), 'yyyy/M/dd'),
                        try_to_date(trim({column_name}), 'yyyy/M/d'),

                        try_to_date(trim({column_name}), 'yyyy/dd/MM'),
                        try_to_date(trim({column_name}), 'yyyy/dd/M'),
                        try_to_date(trim({column_name}), 'yyyy/d/MM'),
                        try_to_date(trim({column_name}), 'yyyy/d/MM'),

                        -- slash, month-first
                        try_to_date(trim({column_name}), 'MM/dd/yyyy'),
                        try_to_date(trim({column_name}), 'M/dd/yyyy'),
                        try_to_date(trim({column_name}), 'MM/d/yyyy'),
                        try_to_date(trim({column_name}), 'M/d/yyyy'),
                        try_to_date(trim({column_name}), 'MM/dd/yy'),
                        try_to_date(trim({column_name}), 'M/dd/yy'),
                        try_to_date(trim({column_name}), 'MM/d/yy'),
                        try_to_date(trim({column_name}), 'M/d/yy'),

                        -- slash, day-first
                        try_to_date(trim({column_name}), 'dd/MM/yyyy'),
                        try_to_date(trim({column_name}), 'dd/MM/yy'),
                        try_to_date(trim({column_name}), 'd/M/yyyy'),
                        try_to_date(trim({column_name}), 'd/M/yy'),
                        try_to_date(trim({column_name}), 'd/MMM/yyyy'),

                        -- dash, year-first
                        try_to_date(trim({column_name}), 'yyyy-MM-dd'),
                        try_to_date(trim({column_name}), 'yyyy-M-d'),
                        try_to_date(trim({column_name}), 'yyyy-dd-MM'),
                        try_to_date(trim({column_name}), 'yyyy-dd-M'),

                        -- dash, month-first
                        try_to_date(trim({column_name}), 'M-dd-yyyy'),
                        try_to_date(trim({column_name}), 'MM-dd-yyyy'),
                        try_to_date(trim({column_name}), 'MM-dd-yy'),
                        try_to_date(trim({column_name}), 'M-dd-yy'),

                        -- dash, day-first
                        try_to_date(trim({column_name}), 'dd-MM-yyyy'),
                        try_to_date(trim({column_name}), 'd-M-yyyy'),
                        try_to_date(trim({column_name}), 'd-M-yy'),
                        try_to_date(trim({column_name}), 'dd-M-yy'),
                        try_to_date(trim({column_name}), 'dd-M-yyyy'),

                        -- dot-separated
                        try_to_date(trim({column_name}), 'dd.MM.yyyy'),
                        try_to_date(trim({column_name}), 'MM.dd.yyyy'),
                        try_to_date(trim({column_name}), 'd.M.yyyy'),
                        try_to_date(trim({column_name}), 'dd.MM.yy'),
                        try_to_date(trim({column_name}), 'yyyy.MM.dd'),
                        try_to_date(trim({column_name}), 'd.MMM.yy'),
                        try_to_date(trim({column_name}), 'd.MMM.yyyy'),

                        -- compact
                        try_to_date(trim({column_name}),'yyyy-MMdd'),
                        try_to_date(trim({column_name}),'ddMM-yyyy'),
                        try_to_date(trim({column_name}), 'dd MM yyyy'),
                        try_to_date(trim({column_name}), 'MM dd yyyy'),
                        try_to_date(trim({column_name}), 'd_M_yyyy'),
                        try_to_date(trim({column_name}), 'ddMMyyyy'),
                        try_to_date(trim({column_name}), 'MMddyyyy'),
                        try_to_date(trim({column_name}), 'yyyyMMdd'),
                        try_to_date(trim({column_name}), 'yyyddMM'),

                        -- month names
                        try_to_date(trim({column_name}), 'dMMMyyyy'),
                        try_to_date(trim({column_name}), 'd-MMM-yyyy'),
                        try_to_date(trim({column_name}), 'dd-MMM-yyyy'),
                        try_to_date(trim({column_name}), 'd MMMM yyyy'),
                        try_to_date(trim({column_name}), 'dd MMM yy'),
                        try_to_date(trim({column_name}), 'dd MMM yyyy'),
                        try_to_date(trim({column_name}), 'MMMM d,yyyy'),
                        try_to_date(trim({column_name}), 'MMMM dd,yyyy'),
                        try_to_date(trim({column_name}), 'MMMM d, yyyy'),
                        try_to_date(trim({column_name}), 'MMMM dd, yyyy'),
                        try_to_date(trim({column_name}), 'MMM/d/yyyy'),
                        try_to_date(trim({column_name}), 'MMM/dd/yyyy'),

                        -- backslash variants via translate + trim
                        try_to_date(trim(translate({column_name}, '\\\\', '/')), 'dd/MM/yyyy'),
                        try_to_date(trim(translate({column_name}, '\\\\', '/')), 'dd/MM/yy'),
                        try_to_date(trim(translate({column_name}, '\\\\', '/')), 'd/M/yyyy'),
                        try_to_date(trim(translate({column_name}, '\\\\', '/')), 'd/M/yy'),
                        try_to_date(trim(translate({column_name}, '\\\\', '/')), 'yyyy/M/d'),
                        try_to_date(trim(translate({column_name}, '\\\\', '/')), 'yyyy/MM/dd'),
                        try_to_date(trim(translate({column_name}, '\\\\', '/')), 'M/d/yyyy')
                    ),
                'yyyy-MM-dd'
                )
                as dob
              from dob_standardization
              """)
    return df

# COMMAND ----------

def map_asset_category(df, column_name):
    _ASSET_MAPPING_CATEGORIES = {
        'CLYMB': 'CLYMB',
        'Clymb': 'CLYMB',
        'CLYMB Yas Island': 'CLYMB',
        'Clymb Entertainment': 'CLYMB',
        'CLYMB Abu Dhabi':'Clymb Abu Dhabi',
        'Etihad Arena': 'Etihad Arena',
        'EAB2C': 'Etihad Arena',
        'Ferrari World Abu Dhabi': 'Ferrari World Abu Dhabi',
        'FWB2C': 'Ferrari World Abu Dhabi',
        'Hilton': 'Hilton Yas Island Abu Dhabi',
        'Hilton AD': 'Hilton Yas Island Abu Dhabi',
        'Hilton Yas Island Abu Dhabi': 'Hilton Yas Island Abu Dhabi',
        'The WB Hotel Abu Dhabi, Curio collection by Hilton': 'WB Hilton',
        'WB Hilton': 'WB Hilton',
        'Warner bros. AD (CMX-OLD)': 'Warner Bros Abu Dhabi',
        'Warner bros. AD CDA Enhancement (DNAS-NEW)': 'Warner Bros Abu Dhabi',
        'Warner Bros': 'Warner Bros Abu Dhabi',
        'Warner bros. AD': 'Warner Bros Abu Dhabi',
        'Warner Bros. World Abu Dhabi': 'Warner Bros Abu Dhabi',
        'WBWB2C': 'Warner Bros Abu Dhabi',
        'Warner Bros World Abu Dhabi':'Warner Bros Abu Dhabi',
        'Yas Mall': 'Yas Mall',
        'Yas-Mall': 'Yas Mall',
        'Yas Waterworld':'Yas Waterworld Abu Dhabi',
        'Yas Bay Waterfront': 'Yas Bay Waterfront',
        'YBW': 'Yas Bay Waterfront',
        'Qasr Experiences' : 'Qasr Al Watan',
        'teamLab Phenomena Abu Dhabi':'Team Lab Phenomena',
        'Team Lab Phenomena':'Team Lab Phenomena',
        'SeaWorld Abu Dhabi':'Sea World Abu Dhabi',
        'UnIdentified': None,
    }
    mapping_expr = F.create_map([F.lit(x) for pair in _ASSET_MAPPING_CATEGORIES.items() for x in pair])
    return df.withColumn(
        f"{column_name}_cleaned",
        F.when(mapping_expr[F.col(column_name)].isNull(), F.col(column_name)).otherwise(mapping_expr[F.col(column_name)])
    ).drop(f"{column_name}").withColumnRenamed(f"{column_name}_cleaned", column_name)

# COMMAND ----------

# DBTITLE 1,check_table_refresh
def check_table_refresh(catalog: str, tables: list, check_date: str = None) -> bool:
    """
    Check if tables have been refreshed (updated/inserted) on or after a given date.

    For each table the freshness check follows this priority:
      1. 'last_updated' tag exists  → uses the tag value (expected YYYY-MM-DD).
      2. No tag, non-view table     → uses DESCRIBE DETAIL (lastModified).
      3. Fallback                   → uses MAX(ts_col) if a timestamp column was provided.
      * Views (vw_*) skip DESCRIBE DETAIL; they MUST be resolved by case 1 or 3.

    A table is considered refreshed if its last modified date >= check_date.

    Args:
        catalog: The catalog name (e.g., 'transformation_dev')
        tables: List of (schema.table, timestamp_column_or_None) tuples
        check_date: Date string in 'YYYY-MM-DD' format. Defaults to today.

    Returns:
        True if all tables are refreshed.

    Raises:
        ValueError: If a view has neither a last_updated tag nor a timestamp column.
        Exception: If any dependency table is NOT refreshed on the check date.
    """
    if check_date is None:
        check_date = datetime.now().strftime('%Y-%m-%d')

    check_date_obj = datetime.strptime(check_date, '%Y-%m-%d').date()

    refresh_status = []
    not_refreshed = []

    for table_name, ts_col in tables:
        full_table_name = f"{catalog}.{table_name}"
        # Extract schema and short table name for tag lookup / view check
        parts = table_name.split(".")
        schema_name = parts[0] if len(parts) > 1 else ""
        table_short = parts[-1]
        is_view = table_short.startswith("vw_")

        try:
            # ── Priority 1: Check for 'last_updated' tag in Unity Catalog ──
            tag_df = spark.sql(f"""
                SELECT tag_value
                FROM system.information_schema.table_tags
                WHERE catalog_name = '{catalog}'
                  AND schema_name  = '{schema_name}'
                  AND table_name   = '{table_short}'
                  AND tag_name     = 'last_updated'
            """)
            tag_rows = tag_df.collect()

            if tag_rows:
                # Handle ISO8601 and other timestamp formats robustly
                tag_value = tag_rows[0]['tag_value']
                try:
                    last_modified_dt = datetime.fromisoformat(tag_value.replace("Z", ""))
                except Exception:
                    last_modified_dt = datetime.strptime(tag_value[:19], '%Y-%m-%dT%H:%M:%S')
                last_modified_str = last_modified_dt.strftime('%Y-%m-%d')
                last_modified_date = last_modified_dt.date()
                is_refreshed = last_modified_date >= check_date_obj
                method = "TAG: last_updated"

            # ── Priority 2: DESCRIBE DETAIL (only for non-views) ──
            elif not is_view:
                detail_df = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
                row = detail_df.collect()[0]
                last_modified = row['lastModified']
                last_modified_str = last_modified.strftime('%Y-%m-%d %H:%M:%S')
                is_refreshed = last_modified.date() >= check_date_obj
                method = "DESCRIBE DETAIL"

            # ── Priority 3: Timestamp column (fallback, required for views) ──
            elif ts_col:
                max_ts_df = spark.sql(
                    f"SELECT CAST(MAX(`{ts_col}`) AS DATE) AS max_date FROM {full_table_name}"
                )
                max_date = max_ts_df.collect()[0]['max_date']
                last_modified_str = str(max_date) if max_date else "NULL"
                is_refreshed = max_date >= check_date_obj if max_date else False
                method = f"MAX({ts_col})"

            else:
                # View with no tag and no timestamp column → cannot determine freshness
                raise ValueError(
                    f"Table '{full_table_name}' is a view (vw_*) — a timestamp column "
                    f"or a 'last_updated' tag must be provided."
                )

            refresh_status.append({
                'table': full_table_name,
                'refreshed_today': is_refreshed,
                'last_modified': last_modified_str,
                'method': method
            })

            if not is_refreshed:
                not_refreshed.append(full_table_name)

        except ValueError:
            raise
        except Exception as e:
            refresh_status.append({
                'table': full_table_name,
                'refreshed_today': False,
                'last_modified': f'ERROR: {str(e)}',
                'method': 'TAG / DESCRIBE DETAIL / ts_col'
            })
            not_refreshed.append(full_table_name)

    # Display results
    display(spark.createDataFrame(refresh_status))

    # Fail orchestration if any table is not refreshed
    if not_refreshed:
        raise Exception(
            f"Dependency check FAILED.\nThe following tables were NOT refreshed on or after {check_date}: "
            f"{', '.join(not_refreshed)}"
        )

    print(f"All {len(tables)} dependency tables are refreshed (on or after {check_date}).")

# COMMAND ----------

# DBTITLE 1,create view
def create_view_or_table(catalog: str, schema: str, object_name: str, select_query: str):
    """
    Creates a view or table based on object_name prefix.
    - 'vw_' prefix: creates a view if not exists.
    - 'tbvw_' prefix: creates or replaces a table.
    Skips if neither prefix is matched.
    """
    if object_name.startswith("vw_"):
        sql_statement = f"CREATE VIEW IF NOT EXISTS {catalog}.{schema}.{object_name} AS {select_query}"
    elif object_name.startswith("tbvw_"):
        sql_statement = f"CREATE OR REPLACE TABLE {catalog}.{schema}.{object_name} AS {select_query}"
    else:
        sql_statement = None
        print(f"Skipping SQL statement for {object_name}, because it is not a view or tbvw")
    if sql_statement:
        print("Executing SQL statement...")
        spark.sql(sql_statement)

# COMMAND ----------

def implement_scd_type2_gss(source_table_ref, target_table_ref, surrogate_keys, primary_key_cols, scd_columns, passthrough_columns=[]):
    """
    Implements SCD Type 2 on Delta tables in Databricks using PySpark.

    Parameters:
    - source_table_ref: Source table reference (catalog.schema.table)
    - target_table_ref: Target table reference (catalog.schema.table)
    - surrogate_keys: Surrogate key column name
    - primary_key_cols: Business key column names
    - scd_columns: List of columns to track changes
    - passthrough_columns: List of columns to propagate from source to target without SCD tracking

    Returns:
    - True if successful
    """
    try:
        source_df = spark.table(source_table_ref)

        source_df = source_df.withColumn(
            "HASHCOL",
            F.sha2(F.concat_ws("|", *[F.trim(F.coalesce(F.col(c).cast("string"), F.lit("NULL"))) for c in scd_columns]), 256)
        )

        current_user_df = spark.sql("SELECT current_user() AS user")
        current_user_value = current_user_df.collect()[0]["user"]

        spark.conf.set("spark.sql.session.timeZone", "Asia/Dubai")
        current_ts_row = spark.sql("SELECT current_timestamp() AS ts").collect()[0]
        current_ts_str = current_ts_row["ts"]
        current_ts = datetime.strftime(current_ts_str, "%Y-%m-%dT%H:%M:%S.%f")[:-3]
        print(current_ts)

        hypothetical_end_date = "9999-12-31T00:00:00.000"

        tables = spark.sql(f"SHOW TABLES IN {target_table_ref.split('.')[0]}.{target_table_ref.split('.')[1]}")
        target_exists = (tables.filter(F.col("tableName") == target_table_ref.split(".")[2]).count() > 0)

        if target_exists:
            target_df = spark.table(target_table_ref)

            join_condition = reduce(
                lambda x, y: x & y,
                [F.col(f"source.{col}").eqNullSafe(F.col(f"target.{col}")) for col in primary_key_cols]
            )

            new_records_df = (
                source_df.alias("source")
                .join(
                    target_df.filter(F.col("current_flag") == 'Y').alias("target"),
                    join_condition,
                    "left"
                )
                .filter(F.col("target.hashcol").isNull())
                .select("source.*")
            )

            new_records_count = new_records_df.count()

            updated_records_df = (
                source_df.alias("source")
                .join(
                    target_df.filter(F.col("current_flag") == 'Y').alias("target"),
                    join_condition,
                    "left"
                )
                .filter(
                    (F.col("target.hashcol").isNotNull()) &
                    (F.col("target.hashcol") != F.col("source.hashcol"))
                )
                .select("source.*")
            )

            updated_records_count = updated_records_df.count()

            updated_and_new_df = new_records_df.unionByName(updated_records_df)
            updated_and_new_df.createOrReplaceTempView("temp_updated_and_new_df")

            join_condition = " AND ".join([f"target.{col} <=> source.{col}" for col in primary_key_cols])
            
            try:
                spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            except Exception:
                pass  # Not available on Serverless; MERGE doesn't require schema evolution here

            scd_columns = [col for col in scd_columns if col not in primary_key_cols]

            merge_query = f"""
            MERGE INTO {target_table_ref} AS target
            USING temp_updated_and_new_df AS source
            ON {join_condition}

            WHEN MATCHED AND target.HASHCOL <> source.HASHCOL AND target.CURRENT_FLAG = 'Y' THEN
            UPDATE SET 
                target.CURRENT_FLAG = 'N',
                target.EFF_END_DATETIME = '{current_ts}',
                target.MODIFIED_DATETIME = '{current_ts}',
                target.MODIFIED_BY = '{current_user_value}'
            """
            spark.sql(merge_query)

            updated_and_new_df = updated_and_new_df \
                .withColumn("CURRENT_FLAG", F.lit('Y')) \
                .withColumn("EFF_START_DATETIME", F.lit(current_ts).cast("timestamp")) \
                .withColumn("EFF_END_DATETIME", F.lit(hypothetical_end_date).cast("timestamp")) \
                .withColumn("CREATED_DATETIME", F.lit(current_ts).cast("timestamp")) \
                .withColumn("MODIFIED_DATETIME", F.lit(current_ts).cast("timestamp")) \
                .withColumn("CREATED_BY", F.lit(current_user_value)) \
                .withColumn("MODIFIED_BY", F.lit(current_user_value))

            updated_and_new_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table_ref)

            return True, new_records_count, updated_records_count

        else:
            new_records_count = source_df.count()
            source_df.createOrReplaceTempView("temp_source_view")

            surrogate_keys_ddl = ",\n".join(
                f"{key} BIGINT GENERATED ALWAYS AS IDENTITY" for key in surrogate_keys
            )

            create_table_query = f"""
            CREATE TABLE {target_table_ref} (
                {surrogate_keys_ddl},
                {fact_schema_ddl},
                {dim_audit_columns_ddl}
            ) USING DELTA
            """
            spark.sql(create_table_query)

            scd_columns = [col for col in scd_columns if col not in primary_key_cols]

            primary_columns = ", ".join([col for col in primary_key_cols])
            scd_columns_expr = ", ".join([col for col in scd_columns])
            passthrough_expr = ", ".join(passthrough_columns)

            default_values_expr = []

            for col_def in fact_schema_ddl.split(","):
                col_name, col_type = col_def.strip().split(None, 1)
                default_values_expr.append(get_default_expr(col_type.upper()))

            for col_def in dim_audit_columns_ddl.split(","):
                parts = col_def.strip().split(None, 1)
                col_name = parts[0]
                col_type = parts[1].upper()

                if col_name == "HASHCOL":
                    default_values_expr.append("'Unknown'" if "STRING" in col_type else f"CAST(NULL AS {col_type})")
                elif col_name == "CURRENT_FLAG":
                    default_values_expr.append("'Y'")
                elif col_name in ["EFF_START_DATETIME", "EFF_END_DATETIME", "CREATED_DATETIME", "MODIFIED_DATETIME"]:
                    default_values_expr.append("TIMESTAMP '1900-01-01 00:00:00'")
                elif col_name in ["CREATED_BY", "MODIFIED_BY"]:
                    default_values_expr.append("'Unknown'")
                else:
                    default_values_expr.append("NULL")

            default_values_expr = [str(value) for value in default_values_expr]

            all_cols = ", ".join([primary_columns, scd_columns_expr, passthrough_expr, "HASHCOL",
                                    "CURRENT_FLAG", "EFF_START_DATETIME", "EFF_END_DATETIME",
                                    "CREATED_DATETIME", "MODIFIED_DATETIME", "CREATED_BY", "MODIFIED_BY"])

            # Build list for SELECT clause with audit values
            select_expr_list = []

            if primary_columns:
                select_expr_list.extend([col.strip() for col in primary_columns.split(",")])
            if scd_columns_expr:
                select_expr_list.extend([col.strip() for col in scd_columns_expr.split(",")])
            if passthrough_expr:
                select_expr_list.extend([col.strip() for col in passthrough_expr.split(",")])

            # Add audit columns
            select_expr_list.extend([
                "HASHCOL",
                f"'Y' AS CURRENT_FLAG",
                f"CAST('{current_ts}' AS TIMESTAMP) AS EFF_START_DATETIME",
                f"CAST('{hypothetical_end_date}' AS TIMESTAMP) AS EFF_END_DATETIME",
                f"CAST('{current_ts}' AS TIMESTAMP) AS CREATED_DATETIME",
                f"CAST('{current_ts}' AS TIMESTAMP) AS MODIFIED_DATETIME",
                f"'{current_user_value}' AS CREATED_BY",
                f"'{current_user_value}' AS MODIFIED_BY"
            ])

            # Final SELECT & INSERT columns
            final_select_expr = ", ".join(select_expr_list)
            final_insert_cols = ", ".join([col for col in all_cols.split(",") if col.strip()])

            # Final query
            first_load_query = f"""
            INSERT INTO {target_table_ref} ({final_insert_cols})
            SELECT {final_select_expr}
            FROM temp_source_view;
            """
            print(f"query_output : {first_load_query}")
            spark.sql(first_load_query)

            print(f"SCD Type 2 processing completed successfully for {target_table_ref}")
            return True, new_records_count, 0

    except Exception as e:
        error_message = f"Error processing SCD Type 2 for {target_table_ref}: {str(e)}"
        print(error_message)
        import traceback
        print(traceback.format_exc())
        raise RuntimeError(error_message)

# COMMAND ----------

# DBTITLE 1,cast_mismatched_columns
def cast_mismatched_columns(df, ddl_struct):
    actual_types = {f.name: f.dataType for f in df.schema.fields}
 
    for field in ddl_struct.fields:
        col_name = field.name
        expected_type = field.dataType
 
        if col_name in actual_types:
            if actual_types[col_name] != expected_type:
                print("Actual column name and type:", col_name, actual_types[col_name])
                print("Expected datatype:", expected_type)
                df = df.withColumn(col_name, col(col_name).cast(expected_type))
 
    return df

# COMMAND ----------

# DBTITLE 1,cast_brz_mismatched_columns
def cast_brz_mismatched_columns(df, ddl_struct):
    # Use case-insensitive lookup: source columns are UPPERCASE, DDL names are CamelCase
    actual_types = {f.name.lower(): (f.name, f.dataType) for f in df.schema.fields}
 
    for field in ddl_struct.fields:
        col_name_lower = field.name.lower()
        expected_type = field.dataType
 
        if col_name_lower in actual_types:
            actual_col_name, actual_type = actual_types[col_name_lower]
            if actual_type != expected_type:
                print("Actual column name and type:", actual_col_name, actual_type)
                print("Expected datatype:", expected_type)
                df = df.withColumn(actual_col_name, col(actual_col_name).cast(expected_type))
 
    return df
