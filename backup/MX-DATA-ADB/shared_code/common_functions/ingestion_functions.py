# Databricks notebook source

# Tables from custom ingestion processes that should be ignored
# when comparing tables to be deleted from the landing schema.
ignore_tables_ingestion = [
    "_sqldf",
    "yww_cleaned",
    "fwad_clean_unpivot",
    "swad_cleaned",
    "yww_clean_unpivot",
    "wbw_clean_unpivot",
    "wbw_cleaned",
    "swad_clean_unpivot",
    "mapping_swad",
    "mapping_yww",
    "mapping_fwad",
    "mapping_wbw",
    "fwad_cleaned",
    "email_launch",
    "email_campaign_data",
    "guest_active_sessions_swad",
    "mdop_fin_wb_tb",
    "mdop_fin_dt_tb",
    "mdop_fin_ht_tb",
    "customer360_email_conversations",
    "customer360_email_participants",
    "customer360_email_sessions",
    "customer360_voice_conversations",
    "customer360_voice_participants",
    "customer360_voice_sessions",
    "gss_yearly_source_market_budget_attendance",
    "gss_yearly_source_market_forecast_attendance"
]

# Subset of the above that should also be ignored when syncing bronze views
# (no corresponding vw_ view is created for these).
ignore_bronze_views = [
    "yww_cleaned",
    "fwad_clean_unpivot",
    "swad_cleaned",
    "yww_clean_unpivot",
    "wbw_clean_unpivot",
    "wbw_cleaned",
    "swad_clean_unpivot",
    "mapping_swad",
    "mapping_yww",
    "mapping_fwad",
    "mapping_wbw",
    "fwad_cleaned"
]

# Schemas that should never be auto-dropped during sync.
ignore_lnd_schemas_delete = {"lnd_genesys"}
ignore_brz_schemas_delete = set()

# COMMAND ----------

from dataclasses import dataclass, field
import re
import time
import random
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional
from pprint import pprint

from databricks.sdk import WorkspaceClient

# COMMAND ----------

# Dataset model
# Represents a single dataset (table/view) tracked across the ingestion
# and transformation catalogs.
#   - datastore_name: schema name in the source/ingestion catalog (e.g. "lnd_sales")
#   - name: table name
#   - location: ABFSS path to the underlying data (only set when discovered from storage)
#   - new_schema_name: corresponding schema name in the transformation catalog
#                       (e.g. "lnd_sales" -> "brz_sales")
@dataclass
class Dataset:
    """Class holding information about datasets."""

    datastore_name: str
    name: str
    location: str = ""
    new_schema_name: str = field(default=None)

    @classmethod
    def from_full_table_name(cls, full_table_name):
        """Build a Dataset from a 'schema.table' string (no location info)."""
        datastore_name, name = full_table_name.split(".")
        return cls(datastore_name, name)

    @classmethod
    def from_location(cls, location):
        """
        Build a Dataset from an ABFSS storage path of the form:
            abfss://<container>/datastore_<name>/dataset_<name>/...
        Used when discovering datasets directly from the data lake.
        """
        pattern = (
            r"^abfss://(?P<container_url>[^/]+)/"
            r"datastore_(?P<datastore>[^/]+)/"
            r"dataset_(?P<dataset>[^/]+)/(\*)?/?$"
        )
        match = re.match(pattern, location)
        if match:
            datastore_name = match.group("datastore")
            datastore_name = 'lnd_' + datastore_name
            return cls(
                datastore_name,
                match.group("dataset"),
                location,
            )
        else:
            raise ValueError(f"Invalid location format: {location}")

    def schema_name(self) -> str:
        """Schema name in the source (ingestion) catalog."""
        return self.datastore_name

    def table_name(self) -> str:
        return self.name

    def full_table_name(self) -> str:
        """schema.table in the source (ingestion) catalog."""
        return f"{self.datastore_name}.{self.name}"

    def full_table_name_with_new_schema(self) -> str:
        """new_schema.vw_table in the transformation catalog."""
        return f"{self.new_schema_name}.vw_{self.name}" if self.new_schema_name else self.full_table_name()

    def __repr__(self) -> str:
        return f"Dataset({self.full_table_name()})"

    def __hash__(self):
        # Hash/equality based on source identity only, so the same dataset
        # discovered twice (e.g. via storage and catalog) compares equal.
        return hash(self.full_table_name())

    def __eq__(self, other):
        if isinstance(other, Dataset):
            return self.full_table_name() == other.full_table_name()
        return NotImplemented


# COMMAND ----------

# Data lake discovery helpers
# Used for initial dataset discovery from storage, not the main sync path.

def get_prefixed_dirs(source_path: str, prefix: str) -> list[str]:
    """Return full paths of immediate child directories starting with `prefix`."""
    return [
        path.path
        for path in dbutils.fs.ls(source_path)
        if (path.name.startswith(prefix) and path.isDir)
    ]


def is_incremental_table(file_name: str) -> bool:
    """True if the file name starts with a date prefix, e.g. '2025_05_01...'."""
    pattern = r"^\d{4}_\d{2}_\d{2}.*"
    return re.match(pattern, file_name) is not None


def is_cdc_table(file_name: str) -> bool:
    """True if the first object in the dataset folder is a Delta transaction log."""
    pattern = r"_delta_log"
    return re.match(pattern, file_name) is not None


def get_first_file(dataset_path: str) -> str:
    """Return the name of the first file/folder under dataset_path."""
    return dbutils.fs.ls(dataset_path)[0].name


def get_datasets_in_storage(root_path: str, ignore_tables=[]) -> set[Dataset]:
    """
    Scan the data lake under root_path and classify each dataset folder as:
      - full-load (plain parquet, no date/delta markers)
      - incremental (date-prefixed folders)
      - CDC/Delta (has a _delta_log folder)
      - masked (couldn't be classified / read error)

    Listing is parallelized since dbutils.fs.ls is a network call per dataset folder.
    Returns four sets of Dataset objects, one per category.
    """
    datasets_in_storage = []
    incremental_datasets_in_storage = []
    cdc_dataset_in_storage = []
    masking_dataset_in_storage = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {}
        for datastore_path in get_prefixed_dirs(root_path, "datastore_"):
            for dataset_path in get_prefixed_dirs(datastore_path, "dataset_"):
                futures[dataset_path] = executor.submit(get_first_file, dataset_path)

        for dataset_path, future in futures.items():
            try:
                first_file = future.result()
                dataset = Dataset.from_location(dataset_path)

                if dataset.table_name() in ignore_tables:
                    print(f"Ignoring table: {dataset.table_name()}")
                    continue

                if is_incremental_table(first_file):
                    print(f"Incremental file: {dataset_path}")
                    incremental_datasets_in_storage.append(dataset)
                elif is_cdc_table(first_file):
                    print(f"Delta file: {dataset_path}")
                    cdc_dataset_in_storage.append(dataset)
                else:
                    print(f"Fullload file: {dataset_path}")
                    datasets_in_storage.append(dataset)
            except Exception:
                # If we can't classify a dataset (e.g. empty folder, permission issue),
                # record it separately rather than failing the whole scan.
                print(f"Masked dataset {dataset_path}")
                masking_dataset_in_storage.append(dataset)

    return (
        set(datasets_in_storage),
        set(incremental_datasets_in_storage),
        set(cdc_dataset_in_storage),
        set(masking_dataset_in_storage),
    )


# COMMAND ----------

# Generic parallel executor with retry/backoff
# Used to apply view create/refresh/drop operations concurrently while
# tolerating transient Unity Catalog API throttling.

def run_parallel(func, items, max_workers=32, max_retries=3, label="task"):
    """
    Run `func(item)` for every item in `items` concurrently using a thread pool.

    Each call is retried with exponential backoff + jitter if the error looks
    like a throttling/rate-limit response (HTTP 429 or "rate"/"throttl" in the
    message) - this matters at high concurrency against Unity Catalog APIs.

    Non-throttling errors are not retried and are recorded immediately.

    Returns:
        dict mapping {item: exception} for every item that ultimately failed.
        An empty dict means everything succeeded.
    """
    failures = {}

    def wrapped(item):
        last_err = None
        for attempt in range(max_retries):
            try:
                func(item)
                return (item, None)
            except Exception as e:
                last_err = e
                msg = str(e).lower()
                if "429" in msg or "rate" in msg or "throttl" in msg:
                    # Backoff: 1s, 2s, 4s... plus random jitter to avoid
                    # synchronized retries across threads.
                    time.sleep((2 ** attempt) + random.random())
                    continue
                # Non-retryable error - fail immediately.
                return (item, e)
        return (item, last_err)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(wrapped, item): item for item in items}
        for future in as_completed(futures):
            item, err = future.result()
            if err:
                failures[item] = err

    print(f"{label}: {len(items)} total, {len(failures)} failed")
    return failures


# COMMAND ----------

# DDL operations: schemas, tables, views

def drop_schema(catalog_name: str, schema_name: str) -> None:
    """Drop a schema. Not used in the automated sync (manual cleanup only)."""
    print(f"Dropping schema: {catalog_name}.{schema_name}")
    sql(f"DROP SCHEMA {catalog_name}.{schema_name}")


def create_schema(catalog_name: str, schema_name: str, root_directory: str) -> None:
    """
    Create a managed schema under the appropriate landing/bronze sub-path
    based on its naming convention (lnd_ / brz_ prefix).
    """
    if schema_name.startswith("lnd_"):
        root_directory = f"{root_directory}/landing"
    elif schema_name.startswith("brz_"):
        root_directory = f"{root_directory}/bronze"
    else:
        raise ValueError(f"Schema {schema_name} does not start with lnd_ or brz_")

    print(f"Creating schema: {catalog_name}.{schema_name}")
    sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name} MANAGED LOCATION '{root_directory}/{schema_name}'")


def drop_table(catalog_name: str, dataset: Dataset) -> None:
    """Drop a managed/external table. Used by refresh_table on schema/type mismatch."""
    print(f"Dropping table: {catalog_name}.{dataset.full_table_name()}")
    sql(f"DROP TABLE {catalog_name}.{dataset.full_table_name()}")


def create_table(catalog_name: str, dataset: Dataset, type: str) -> None:
    """
    Create an external table pointing at the dataset's storage location.
    `type` should be 'PARQUET' or 'DELTA'.
    """
    print(f"Creating table ({type}): {catalog_name}.{dataset.full_table_name()}")
    sql(
        f"""
        CREATE TABLE {catalog_name}.{dataset.full_table_name()}
        USING {type}
        LOCATION '{dataset.location}';
    """
    )


def create_view(ingestion_catalog_name: str, transformation_catalog_name: str, dataset: Dataset) -> None:
    """
    Create a bronze view in the transformation catalog that selects everything
    from the corresponding table in the ingestion catalog.

    Uses CREATE VIEW IF NOT EXISTS so it's safe to call even if the view
    already exists (no-op in that case).
    """
    print(f"Creating view: {dataset.full_table_name_with_new_schema()}")
    sql(
        f"""
        CREATE VIEW IF NOT EXISTS {transformation_catalog_name}.{dataset.new_schema_name}.vw_{dataset.table_name()} AS
        SELECT * FROM {ingestion_catalog_name}.{dataset.full_table_name()};
        """
    )


def drop_view(catalog_name: str, full_table_name: str) -> None:
    """Drop a view. full_table_name should be 'schema.vw_table'."""
    print(f"Dropping view: {full_table_name}")
    sql(
        f"""
        DROP VIEW {catalog_name}.{full_table_name};
        """
    )


def refresh_view(ingestion_catalog_name: str, transformation_catalog_name: str, dataset: Dataset) -> None:
    """
    Recreate a bronze view using CREATE OR REPLACE VIEW.

    Intended to be called ONLY for views that have already been confirmed
    (via a bulk information_schema column comparison upstream) to need
    refreshing - it does not perform its own schema check.

    CREATE OR REPLACE is preferred over DROP + CREATE because it:
      - is a single statement (fewer round trips at scale)
      - preserves existing grants on the view object
      - preserves existing Unity Catalog tags on the view object
    """
    print(f"Refreshing view: {dataset.full_table_name_with_new_schema()}")
    sql(
        f"""
        CREATE OR REPLACE VIEW {transformation_catalog_name}.{dataset.new_schema_name}.vw_{dataset.table_name()} AS
        SELECT * FROM {ingestion_catalog_name}.{dataset.full_table_name()};
        """
    )


def update_last_updated_tag(catalog_name: str, full_table_name: str) -> None:
    """
    Stamp a table/view with a 'last_updated' tag (current time, Asia/Dubai).
    Used for observability / freshness tracking. Not called in the main sync
    path by default - available for callers that need it.
    """
    try:
        dt = datetime.now(ZoneInfo("Asia/Dubai"))
        timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "0"

        spark.sql(f"""
            ALTER TABLE {catalog_name}.{full_table_name}
            SET TAGS ('last_updated' = '{timestamp}')
        """)
        print(f"Tag 'last_updated' set on {full_table_name} with value {timestamp}")
    except Exception as e:
        print(f"Failed to set tag on {full_table_name}: {str(e)}")
        raise


# COMMAND ----------

# Table type / schema reconciliation
# Handles drift in the underlying ingestion tables themselves, separate from
# the bronze view sync.

def get_expected_table_type(dataset: Dataset):
    """
    Inspect the dataset's storage location and determine what its catalog
    table SHOULD look like:
        - CDC/Delta data        -> ('delta', 'EXTERNAL')
        - date-prefixed/incr.   -> ('parquet', 'MANAGED')
        - plain full-load       -> ('parquet', 'EXTERNAL')
    """
    files = dbutils.fs.ls(dataset.location)
    first_file_name = files[0].name

    if is_cdc_table(first_file_name):
        return 'delta', 'EXTERNAL'
    elif is_incremental_table(first_file_name):
        return 'parquet', 'MANAGED'
    else:
        return 'parquet', 'EXTERNAL'


def refresh_table(catalog_name: str, dataset: Dataset) -> None:
    """
    Reconcile a single ingestion table against its expected type/schema:

    1. If the catalog table's type (EXTERNAL/MANAGED, parquet/delta) doesn't
       match what the storage layout implies, drop and recreate the table.
    2. Otherwise, compare the storage schema to the catalog schema and
       drop/recreate if they've diverged (e.g. new columns added upstream).

    Per-table operation - not parallelized since it involves reading data to
    infer schema (spark.read.load), which is heavier than the metadata-only
    view sync operations.
    """
    format, expected_table_type = get_expected_table_type(dataset)

    try:
        table_type = spark.catalog.getTable(f"{catalog_name}.{dataset.full_table_name()}").tableType
        table_location = spark.sql(f"desc formatted {catalog_name}.{dataset.full_table_name()}").filter("col_name=='Location'").collect()[0].data_type
    except Exception as e:
        # If the table was previously Delta but the path no longer exists,
        # getTable() throws DELTA_PATH_DOES_NOT_EXIST - treat as a Delta table
        # so the type-mismatch branch below handles the recreate.
        if "DELTA_PATH_DOES_NOT_EXIST" in str(e):
            table_type = 'delta'
            table_location = ''
        else:
            raise

    if table_type != expected_table_type:
        print(f"Tabletype mismatch for {dataset.full_table_name()}. Catalog table is {table_type}, should be {expected_table_type}. Dropping and recreating.")
        drop_table(catalog_name, dataset)
        create_table(catalog_name, dataset, format.upper())
    else:
        # Type matches - check for column-level schema drift.
        schema_in_storage = spark.read.load(dataset.location, format=format).schema
        schema_in_catalog = spark.read.table(f"{catalog_name}.{dataset.full_table_name()}").schema

        if schema_in_storage != schema_in_catalog:
            print(
                f"""
                Update schema for {dataset.full_table_name()}
                From \n{schema_in_catalog} to \n{schema_in_storage}
                """
            )
            drop_table(catalog_name, dataset)
            create_table(catalog_name, dataset, format.upper())


# COMMAND ----------

# Catalog metadata helpers (information_schema based)
#
# These replace per-object SHOW TABLES / DESCRIBE / getTable() calls with
# single bulk queries, which is essential for syncing thousands of views
# in a reasonable amount of time.

def get_schemas_in_catalog(catalog_name: str) -> set[str]:
    """
    Return all user schema names in a catalog (excluding system/reserved schemas),
    via a single information_schema.schemata query.
    """
    df = spark.sql(f"""
        SELECT schema_name
        FROM {catalog_name}.information_schema.schemata
    """).toPandas()
    return set(df["schema_name"].values) - {"information_schema", "default", "etl_audit", "volumes"}


def get_tables_in_catalog(catalog_name: str, ignore_tables=[]) -> set[Dataset]:
    """
    Return all tables/views across all user schemas in a catalog as Dataset
    objects, via a single information_schema.tables query (instead of one
    SHOW TABLES call per schema).
    """
    ignore_tables = set(ignore_tables or [])
    schemas = get_schemas_in_catalog(catalog_name)
    if not schemas:
        return set()

    schema_filter = ", ".join(f"'{s}'" for s in schemas)

    df = spark.sql(f"""
        SELECT table_schema, table_name
        FROM {catalog_name}.information_schema.tables
        WHERE table_schema IN ({schema_filter})
    """).toPandas()

    return {
        Dataset.from_full_table_name(f"{row.table_schema}.{row.table_name}")
        for _, row in df.iterrows()
        if row.table_name not in ignore_tables
    }


def get_table_types(catalog_name: str, schemas: set[str]) -> dict:
    """
    Return {schema.table: table_type} (e.g. 'VIEW', 'MANAGED', 'EXTERNAL')
    for the given schemas, via a single information_schema.tables query.

    Used to:
      - confirm an object slated for removal is actually a VIEW before dropping it
      - confirm a refresh target isn't actually a TABLE before replacing it as a view
    """
    if not schemas:
        return {}
    schema_filter = ", ".join(f"'{s}'" for s in schemas)
    df = spark.sql(f"""
        SELECT table_schema, table_name, table_type
        FROM {catalog_name}.information_schema.tables
        WHERE table_schema IN ({schema_filter})
    """).toPandas()
    return {
        f"{row.table_schema}.{row.table_name}": row.table_type
        for _, row in df.iterrows()
    }


def get_columns_map(catalog_name: str, schemas: set[str]) -> dict:
    """
    Return {schema.table: [(column_name, full_data_type), ...]} (ordered by
    ordinal position) for every table in the given schemas, via a single
    information_schema.columns query.

    This replaces a per-object DESCRIBE call and is the basis for detecting
    schema drift between an ingestion table and its corresponding bronze view.
    """
    if not schemas:
        return {}
    schema_filter = ", ".join(f"'{s}'" for s in schemas)
    df = spark.sql(f"""
        SELECT table_schema, table_name, column_name, full_data_type, ordinal_position
        FROM {catalog_name}.information_schema.columns
        WHERE table_schema IN ({schema_filter})
        ORDER BY table_schema, table_name, ordinal_position
    """).toPandas()

    result = {}
    for (schema, table), group in df.groupby(["table_schema", "table_name"]):
        result[f"{schema}.{table}"] = list(zip(group["column_name"], group["full_data_type"]))
    return result


# COMMAND ----------

# Misc helpers

def print_failures(fail_dict):
    """Pretty-print a {item: exception} failures dict produced by run_parallel."""
    if fail_dict:
        print(f"Failures: {len(fail_dict)}")
        print(fail_dict.keys())
        for k, v in fail_dict.items():
            print("#" * 80)
            print(k)
            print(v)


# Job control helpers - used to prevent overlapping sync runs from piling up
# if the job is triggered more frequently than it can complete.

def get_workspace_client() -> WorkspaceClient:
    """Create a Databricks SDK workspace client using the notebook's auth context."""
    return WorkspaceClient()


def find_job(workspace_client: WorkspaceClient, job_name: str) -> Optional[Dict]:
    """Find a job definition by its display name."""
    return next((job for job in workspace_client.jobs.list() if job.settings.name == job_name), None)


def get_job_details(job_name: str) -> Dict:
    """
    Look up a job by name and return its id/name.
    Raises ValueError if no job with that name exists.
    """
    workspace_client = get_workspace_client()
    job = find_job(workspace_client, job_name)

    if not job:
        raise ValueError(f"No job found with name: {job_name}")

    return {
        "job_id": job.job_id,
        "job_name": job.settings.name
    }


def cancel_runs_after_first_two(job_name: str) -> None:
    """
    If multiple runs of `job_name` are active simultaneously (queued or running),
    cancel all but the two oldest. This prevents a backlog of sync runs from
    building up if the job is triggered faster than it completes.
    """
    job = get_job_details(job_name)
    workspace_client = get_workspace_client()

    runs = list(workspace_client.jobs.list_runs(job_id=job["job_id"], active_only=True))

    runs_sorted = sorted(runs, key=lambda r: r.start_time)
    for run in runs_sorted[2:]:
        print(f"Cancelling {run.run_id}")
        workspace_client.jobs.cancel_run(run_id=run.run_id)


# Backup / point-in-time read helper

def read_table_or_backup(table_name: str, environment: str, date: str = None):
    """
    Read a table either from Unity Catalog (current state) or from a daily
    backup snapshot in storage (if `date` is provided).

    Args:
        table_name: Full table name, e.g. "transformation_dev.brz_scv.vw_table".
        environment: Environment identifier (e.g. "dev", "prod") - used to
                      construct the backup storage account name.
        date: Optional date prefix (e.g. "2024-01-15") identifying which
              day's backup folder to read. If omitted, reads live from UC.

    Returns:
        pyspark.sql.DataFrame
    """
    backup_root = f"abfss://daily@stmxdatabackup{environment}01.dfs.core.windows.net/databricks"

    if date:
        table_path = f"{backup_root}/{table_name.replace('.', '/')}"

        # Backup folders are timestamped subfolders prefixed with the date,
        # e.g. "2024-01-15_03-00-00". Pick the latest one for that date.
        folders = [
            f.name.rstrip('/')
            for f in dbutils.fs.ls(table_path)
            if f.name.startswith(date)
        ]

        if not folders:
            raise FileNotFoundError(f"No backup folders found for table {table_name} and date {date}")

        latest_folder = sorted(folders)[-1]
        backup_path = f"{table_path}/{latest_folder}"

        print(f"Reading from backup path: {backup_path}")
        df = spark.read.format("parquet").load(backup_path)
    else:
        print(f"Reading from Unity Catalog table: {table_name}")
        df = spark.table(table_name)

    return df

# Example usage:
# df = read_table_or_backup("transformation_dev.brz_scv.vw_dbo_t_mdm_guest", "dev", "2025-07-30")