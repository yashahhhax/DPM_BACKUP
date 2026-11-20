# Databricks notebook source
# DBTITLE 1,send_teams_notification
import requests
import json
from datetime import datetime, timedelta, date, timezone
 
def send_teams_notification(refresh_status_df, webhook_url, usecase, env):
    """
    Sends individual Power BI refresh status messages to Teams using MessageCard format.
    """
    # Collect all rows from the Spark DataFrame
    rows = refresh_status_df.collect()
   
    # Get current time in UTC
    utc_now = datetime.now(timezone.utc)
    # Convert to UAE time (UTC+4)
    uae_time = utc_now + timedelta(hours=4)
    # Format as string
    notification_time = uae_time.strftime("%m/%d/%Y %H:%M:%S")
 
    for i, row in enumerate(rows, 1):
        r = row.asDict()
        status = r.get("status", "").strip().lower()
        report_name = r.get("report_name", "Report")
        if status == "completed":
            status_message = f"Report '{report_name}' refresh successful."
        else:
            status_message = (f"Report '{report_name}' refresh failed. "
                            f"Please navigate to {usecase} master pipeline and investigate for more details.")
        emoji = "✅" if status == "completed" else "❌"
        status = "SUCCESS" if status == "completed" else status.upper()
 
        status_field = {"name": "Status:", "value": f"{emoji} {status}"}
 
        # Safely format refresh_date
        refresh_date = r.get("refresh_date", "N/A")
        if isinstance(refresh_date, (datetime, date)):
            refresh_date = refresh_date.strftime("%Y-%m-%d")
 
        # Build MessageCard payload for each report
        message = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": f"Power BI Dataset Refresh Status - {r.get('report_name', 'N/A')}",
            "themeColor": "28a745" if status == "SUCCESS" else "ff0000",
            "title": f"📊 Power BI Dataset Refresh Status — {usecase.upper()} | {env.upper()}",
            "sections": [{
                "facts": [
                    {"name": "Report Name:", "value": r.get("report_name", "N/A")},
                    status_field,
                    {"name": "Duration:", "value": r.get("refresh_duration_hms", "-")},
                    {"name": "Error:", "value": r.get("error_message") or "None"},
                    {"name": "Refresh Date:", "value": refresh_date},
                    {"name": "Message:", "value": status_message},
                    {"name": "Notification Time (UAE):", "value": notification_time},
                ],
                "markdown": True
            }]
        }
 
        # Send the message
        response = requests.post(
            url=webhook_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(message)
        )
 
        # Log the result
        if response.status_code in [200, 202]:
            print(f"Notification {i} sent successfully.")
        else:
            print(f"Failed to send notification {i}.")
            print("Status code:", response.status_code)
            print("Response:", response.text)

# COMMAND ----------

# DBTITLE 1,send_powerbi_refresh_summary
from datetime import datetime, timedelta

import requests
import json

def send_powerbi_refresh_summary(refresh_status_df, webhook_url, usecase, env):
    """
    Sends Power BI refresh status summary to Teams via workflow webhook.
    
    Parameters:
    - refresh_status_df (DataFrame): PySpark DataFrame containing refresh log data
    - webhook_url (str): Teams workflow webhook URL
    """
    # Collect rows from DataFrame
    # display(refresh_status_df)
    rows = refresh_status_df.collect()
    # print(rows)

    # Compose markdown-like table
    header = "\n" + f"📊 **Power BI Dataset Refresh Status - Summary** —  **{usecase.upper()}** | **{env.upper()}**\n\n"
    
    table_header = (
        "| No. | Refresh Date | Report Name             | Dataset ID                         | Status     | Error                      | Start Time          | End Time            | Duration |\n"
    )
    table_divider = (
        "|-----|---------------|-------------------------|-------------------------------------|------------|----------------------------|----------------------|----------------------|----------|\n"
    )

    table_rows = ""
    for i, row in enumerate(rows, 1):
        r = row.asDict() if row is not None else {}
        status_raw = r.get("status", "").strip().lower()
        emoji = "✅" if status_raw == "completed" else "❌"
        status_display = f"{emoji} {r.get('status', 'N/A')}"
        error_message = (str(r.get('error_message')) if r.get('error_message') else '-')[:26]
        table_rows += (
            f"| {i:<3} | {str(r.get('refresh_date') or '-'):<13} | {r.get('report_name', '-'):<23} "
            f"| {r.get('dataset_id', '-'):<35} | {status_display:<15} | {error_message:<26} "
            f"| {str(r.get('start_time', '-')):<20} | {str(r.get('end_time', '-')):<20} | {r.get('refresh_duration_hms', '-'):<8} |\n"
        )


    footer = f"\n📩 **Notified At:** {(datetime.utcnow() + timedelta(hours=4)).strftime('%Y-%m-%d %H:%M:%S UAE')}"

    # Final message
    message_payload = {
        "text": header + table_header + table_divider + table_rows + footer
    }

    # Send to Teams via Workflow Webhook
    response = requests.post(
        url=webhook_url,
        headers={"Content-Type": "application/json"},
        data=json.dumps(message_payload)
    )

    # Check result
    if response.status_code in [200, 202]:
        print("✅ Notification sent successfully.")
    else:
        print("❌ Failed to send notification:", response.status_code)
        print(response.text)
