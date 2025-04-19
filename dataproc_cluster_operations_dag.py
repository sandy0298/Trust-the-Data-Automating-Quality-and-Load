from airflow import models
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.trigger_rule import TriggerRule

# Replace with your actual values
PROJECT_ID = "river-lane-453009-h7"
CLUSTER_NAME = "low-cost-dataproc-cluster"
REGION = "us-central1"
PYSPARK_URI = "gs://us-central1-sandy-environme-adc095c4-bucket/dags/spark_job.py"

SOURCE_BUCKET = "gs://time_data01/"
DEST_BUCKET = "gs://archival-file/"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
}

# Function to send email via Gmail
def send_email_via_gmail(status, reason, file_name):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender_email = "sandeepmohanty024@gmail.com"
    sender_password = "your app password"
    recipient_emails = ["sandeep.mohanty1998@gmail.com"]

    if status.lower() == "success":
        subject = "airflow dag success - file load to bigquery is complete"
    else:
        subject = "airflow dag failed - file load to bigquery is incomplete"

    body = f"""
    <h3>DAG Execution Notification</h3>
    <p><b>Status:</b> {status}</p>
    <p><b>File:</b> {file_name}</p>
    <p><b>Details:</b> {reason}</p>
    """

    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipient_emails)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_emails, msg.as_string())
            print("Notification email sent.")
    except Exception as e:
        print(f"Email sending failed: {e}")


# Task function to check DAG run status and trigger the email
def notify_by_email(**context):
    dag_run = context["dag_run"]
    failed_tasks = [t for t in dag_run.get_task_instances() if t.state == "failed"]

    if failed_tasks:
        status = "FAILED"
        reason = f"{len(failed_tasks)} task(s) failed: " + ", ".join([t.task_id for t in failed_tasks])
    else:
        status = "SUCCESS"
        reason = "All tasks completed successfully."

    send_email_via_gmail(status, reason, "spark_job.py")

# DAG Definition
with models.DAG(
    "dataproc_cluster_operations",
    schedule_interval=None,
    start_date=None,
    catchup=False
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    PYSPARK_JOB = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
    }

    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    file_archival = BashOperator(
        task_id="file_archival",
        bash_command=f"""
        for file in $(gsutil ls {SOURCE_BUCKET}); do
            echo "Archiving $file..."
            gsutil mv "$file" {DEST_BUCKET}
        done
        """
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION
    )

    email_notification = PythonOperator(
        task_id="email_notification",
        python_callable=notify_by_email,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of previous task outcomes
    )

    # DAG Task Flow
    create_cluster >> pyspark_task >> file_archival >> delete_cluster >> email_notification
