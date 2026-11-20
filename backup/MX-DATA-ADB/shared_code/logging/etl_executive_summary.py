# Databricks notebook source
dbutils.widgets.text("env","")
dbutils.widgets.text("receiver_email_id","")
dbutils.widgets.text("secret_scope", "kv-mx-data-adf-inges-dev")
dbutils.widgets.text("email_key","")

# COMMAND ----------

# MAGIC %run ../common_functions/base_functions

# COMMAND ----------

from datetime import datetime

try:
    notebook_vars = ['env','receiver_email_id', 'secret_scope', 'email_key']
    
    # Unpack with defaults as None
    env,receiver_email_id,secret_scope,email_key = get_notebook_params(*notebook_vars, verbose=True)

except Exception as e:
    print(f"[ERROR] {str(e)}")
    raise


# COMMAND ----------

from pyspark.sql.functions import col, lower, lit, current_date, current_timestamp, max as spark_max
from datetime import datetime, timedelta

# COMMAND ----------

usecase_workflow_map = {
    'travelbox': ['wf_master_travelbox'],
    # 'maintenance': ['wf_master_maintenance', 'wf_maint_ride_daily'],
    'maintenance_ride': ['wf_maint_ride_daily'],
    'maintenance_maximo': ['wf_master_maintenance'],
    'ea_drr': ['wf_master_ea_drr'],
    'oracle_gl': ['wf_master_oracle_gl'],
    'google_analytics': ['wf_master_google_analytics'],
    'ridecollation': ['wf_master_ridecollation'],
    'emarsys' : ['wf_master_emarsys'],
    'scv':['wf_master_scv'],
    'ipera': [],
    'drr':[],
    'customer360':[],
    'gss':['wf_master_gss_questionpro','wf_master_gss_social_media','wf_master_gss_surveyhero','wf_master_gss_budget_attendance','wf_master_gss_powerbi_refresh']
}

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import lower, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize workspace client
w = WorkspaceClient()

summary_data = []

for usecase, master_jobs in usecase_workflow_map.items():
    for master_job in master_jobs:   # iterate through each workflow
        
        # 1. Read all logs from last 24 hours
        df_24h = spark.sql(f"""
            SELECT *
            FROM transformation_{env}.etl_audit.etl_audit_logs_all
            WHERE lower(use_case) = '{usecase}'
              AND lower(job_name) = '{master_job}'
              AND log_timestamp >= current_timestamp() - INTERVAL 24 HOURS
        """)

        # 2. Update null result_state by calling actual run status using SDK
        df_24h_collect = df_24h.collect()
        updated_rows = []

        for row in df_24h_collect:
            result_state = row["result_state"]
            job_run_id = row["job_run_id"]

            if result_state is None and job_run_id is not None:
                try:
                    run = w.jobs.get_run(run_id=int(job_run_id))
                    result_state = run.state.result_state.name if run.state.result_state else "RUNNING"
                except Exception:
                    result_state = "UNKNOWN"

            updated_rows.append(row.asDict() | {"result_state": result_state})

        # Infer schema from original df_24h
        inferred_schema = df_24h.schema
        df_24h_updated = spark.createDataFrame(updated_rows, schema=inferred_schema)

        # 3. Compute counts
        success_states = ["success", "succeeded"]

        total_24h = df_24h_updated.count()
        success_count = df_24h_updated.filter(lower(col("result_state")).isin(success_states)).count()
        failed_count = df_24h_updated.filter(~lower(col("result_state")).isin(success_states)).count()

        # 4. Get the latest execution (most recent run)
        # df_latest = spark.sql(f"""
        #     SELECT job_run_id, result_state, log_timestamp
        #     FROM transformation_{env}.etl_audit.etl_audit_logs_all
        #     WHERE lower(use_case) = '{usecase}'
        #       AND lower(job_name) = '{master_job}'
        #     ORDER BY log_timestamp DESC
        #     LIMIT 1
        # """)
        df_latest = spark.sql(f"""
                SELECT
                    job_run_id,
                    result_state,
                    from_utc_timestamp(
                        to_utc_timestamp(log_timestamp, 'Asia/Kolkata'),
                        'Asia/Dubai'
                    ) AS log_timestamp
                FROM transformation_{env}.etl_audit.etl_audit_logs_all
                WHERE lower(use_case) = '{usecase}'
                AND lower(job_name) = '{master_job}'
                ORDER BY log_timestamp DESC
                LIMIT 1
         """)



        if df_latest.count() > 0:
            latest_row = df_latest.collect()[0]
            job_run_id = latest_row["job_run_id"]
            last_time = latest_row["log_timestamp"]

            try:
                job_run = w.jobs.get_run(run_id=int(job_run_id))
                actual_result_state = job_run.state.result_state.name if job_run.state.result_state else "RUNNING"
            except Exception:
                actual_result_state = latest_row["result_state"] or "N/A"
        else:
            actual_result_state = "N/A"
            last_time = "N/A"

        # 5. Append to summary (separate row for each workflow)
        summary_data.append((
            usecase,
            master_job,
            total_24h,
            success_count,
            failed_count,
            actual_result_state,
            str(last_time)
        ))
    
    if not master_jobs:
        
        drr_filter = "1=0"
        if usecase == 'drr':
            print("yes")
            drr_filter = """lower(use_case) like '%drr%'
                    AND lower(use_case) <> 'ea_drr'
                    AND lower(job_name) like '%master%'"""

        final_df = spark.sql(f"""
                  WITH latest_per_workflow AS (
                    -- Get the latest run of each workflow per use case (last 24h)
                    SELECT
                        CASE WHEN '{usecase}' = 'drr' THEN 'drr'
                        ELSE use_case
                        END as  use_case,
                        job_name,
                        job_id,
                        job_run_id,
                        CASE
                            WHEN result_state IS NULL OR TRIM(result_state) = '' THEN 'RUNNING'
                            ELSE UPPER(result_state)
                        END AS result_state,
                        state_message,
                        source,
                        from_utc_timestamp(log_timestamp, 'Asia/Dubai') AS log_timestamp_dubai
                    FROM transformation_{env}.etl_audit.etl_audit_logs_all
                    WHERE  1=1
                    AND  log_timestamp >= current_timestamp() - INTERVAL 24 HOURS
                    AND (
                        ('{usecase}' = 'drr' AND {drr_filter})
                        OR ('{usecase}' <> 'drr' AND lower(use_case) = '{usecase}')
                    )
                    QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY use_case, job_name
                            ORDER BY log_timestamp DESC
                        ) = 1
                ),
                usecase_summary AS (
                    -- Aggregate at use case level
                    SELECT
                        use_case,
                        COUNT(*) AS total_workflows,
                        SUM(CASE WHEN lower(result_state) in ("success", "succeeded") THEN 1 ELSE 0 END) AS success_count,
                        SUM(CASE WHEN lower(result_state) not in ("success", "succeeded") THEN 1 ELSE 0 END) AS failed_count,
                        CASE
                            WHEN SUM(CASE WHEN lower(result_state) not in ("success", "succeeded") THEN 1 ELSE 0 END) > 0
                            THEN 'FAILED'
                            ELSE 'SUCCESS'
                        END AS overall_status,
                        MAX(log_timestamp_dubai) AS last_execution_time
                    FROM latest_per_workflow
                    GROUP BY use_case
                )
                SELECT * FROM usecase_summary
                  """)
        if final_df.count() == 0:
            summary_data.append((
                usecase,
                'N/A',
                0,
                0,
                0,
                'SUCCESS',
                'N/A'
            ))
        else:
            for row in final_df.collect():
                summary_data.append((
                    usecase,
                    'N/A',
                    row["total_workflows"],
                    row["success_count"],
                    row["failed_count"],
                    row["overall_status"],
                    str(row["last_execution_time"])
                ))

# 6. Schema with Master Workflow
schema = StructType([
    StructField('Use Case', StringType(), True),
    StructField('Master Workflow', StringType(), True),
    StructField('Total Runs (last 24h)', IntegerType(), True),
    StructField('Total Successful runs (last 24h)', IntegerType(), True),
    StructField('Total Failed runs (last 24h)', IntegerType(), True),
    StructField('Last Execution Status', StringType(), True),
    StructField('Last Execution Time', StringType(), True)
])

summary_df = spark.createDataFrame(summary_data, schema)

# 7. Display Summary
display(summary_df)


# COMMAND ----------

# DBTITLE 1,Cell 7
from pyspark.sql.functions import col,lower
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# -------- Step 1: Convert summary_df grouped by Use Case into HTML --------
statuses = [
    (row["Last Execution Status"] or "N/A").upper()
    for row in summary_df.select("Last Execution Status").collect()
]

summary_df_colored = summary_df.withColumn(
    "Last Execution Status",
    when(col("Last Execution Status") == "SUCCESS",
         "<span style='color:green;font-weight:bold;'>SUCCESS</span>")
    .when(col("Last Execution Status").isin("FAIL", "FAILED", "ERROR"),
         "<span style='color:red;font-weight:bold;'>FAIL</span>")
    .when(col("Last Execution Status").isin("SUCCESS_WITH_FAILURES"),
         "<span style='color:gray;font-weight:bold;'>SUCCESS WITH FAILURES</span>")
    .otherwise(
         "<span style='color:orange;font-weight:bold;'>N/A</span>")
)

summary_html = summary_df_colored.toPandas().to_html(
    index=False,
    escape=False
)

# Combine all sections into final HTML body
message_body = f"""
<html>
<head>
    <style>
        table {{
            border-collapse: collapse;
            width: 100%;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
        }}
        th {{
            background-color: #f2f2f2;
            text-align: left;
        }}
    </style>
</head>
<body>

<h3>Daily Use Case wise ETL Workflow Executive Summary</h3>

<br>

{summary_html}

</body>
</html>
"""

# -------- Step 2: Email sending logic --------
def send_email(sender_email, sender_password, receiver_email, subject, html_content):
    try:
        msg = MIMEMultipart("alternative")
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject

        msg.attach(MIMEText(html_content, 'html'))

        with smtplib.SMTP('smtp-mail.outlook.com', 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())

        print(" Email sent successfully!")

    except Exception as e:
        print(f" Error sending email: {e}")

# Credentials (use Databricks secret for password)
sender_email = "Data_team_notifications@miral.ae"
sender_password = dbutils.secrets.get(scope=secret_scope, key=f"{email_key}")
receiver_email = f"{receiver_email_id}"

# Subject line
today = datetime.today().strftime("%Y-%m-%d")
subject = f"{env.upper()} Executive ETL Summary {today}"

# Send the email
#send_email(sender_email, sender_password, receiver_email, subject, message_body)


if isinstance(receiver_email, str):
    recipient_list = [e.strip() for e in receiver_email.split(',') if e.strip()]
    #send_email(sender_email, sender_password, receiver_email, subject, message_body)
    #display(recipient_list)
else:
    recipient_list = [e.strip() for e in receiver_email if e.strip()]

for email in recipient_list:
    #print(email)
    send_email(sender_email, sender_password, email, subject, message_body)
