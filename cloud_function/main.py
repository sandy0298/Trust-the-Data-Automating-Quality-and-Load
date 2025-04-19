import functions_framework
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage
import pandas as pd
import io
import requests
from datetime import datetime
from email.mime.text import MIMEText
import smtplib

# ---------- Email Function ----------
def send_email_via_gmail(failure_reason, file_name):
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender_email = "sandeepmohanty024@gmail.com"
    sender_password = "your app password"
    recipient_emails = ["sandeep.mohanty1998@gmail.com"]

    subject = f"File Validation Failed: {file_name}"
    body = f"""
    <html><body>
        <p><b>Validation failed for file:</b> {file_name}</p>
        <p><b>Date of failure:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><b>Reason:</b> {failure_reason}</p>
        <h3>Failure Details:</h3>
        <table border="1" cellpadding="5" cellspacing="0">
            <tr><th>Date</th><th>File Name</th><th>Reason</th></tr>
            <tr><td>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</td>
            <td>{file_name}</td><td>{failure_reason}</td></tr>
        </table>
    </body></html>
    """
    msg = MIMEText(body, "html")
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = ", ".join(recipient_emails)

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        print(f"Email sent successfully to {', '.join(recipient_emails)}.")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")


# ---------- Validation Functions ----------
def validate_file_name(file_name):
    if not file_name.lower().endswith(".csv"):
        raise ValueError(f"Invalid file format: {file_name}. Expected .csv")

def validate_file_empty(bucket, file_name):
    blob = bucket.blob(file_name)
    data = blob.download_as_text()
    if not data.strip():
        raise ValueError(f"File {file_name} is empty.")
    df = pd.read_csv(io.StringIO(data))
    if df.empty:
        raise ValueError(f"File {file_name} contains no rows.")
    return df

def validate_columns(df, expected_columns):
    file_columns = df.columns.tolist()
    missing = [col for col in expected_columns if col not in file_columns]
    extra = [col for col in file_columns if col not in expected_columns]
    if missing:
        raise ValueError(f"Missing columns: {', '.join(missing)}")
    if extra:
        raise ValueError(f"Unexpected columns present: {', '.join(extra)}")

def validate_file_size(bucket, file_name, max_size_mb=100):
    blob = bucket.blob(file_name)
    blob.reload()
    size_mb = blob.size / (1024 * 1024)
    if size_mb > max_size_mb:
        raise ValueError(f"File too large: {size_mb:.2f} MB (max: {max_size_mb} MB)")


# ---------- DAG Trigger Logic ----------
AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])
WEB_SERVER_URL = "https://custom_airflow_webserver_url-us-central1.composer.googleusercontent.com"
DAG_ID = "dataproc_cluster_operations"

def make_composer2_web_server_request(url: str, method: str = "GET", **kwargs):
    authed_session = AuthorizedSession(CREDENTIALS)
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90
    return authed_session.request(method, url, **kwargs)

def trigger_dag(web_server_url: str, dag_id: str, data: dict) -> str:
    endpoint = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{web_server_url}/{endpoint}"
    json_data = {"conf": data}
    response = make_composer2_web_server_request(
        request_url, method="POST", json=json_data
    )
    if response.status_code == 403:
        raise requests.HTTPError("Permission denied: " + response.text)
    elif response.status_code != 200:
        response.raise_for_status()
    return response.text


# ---------- Cloud Function Entry Point ----------
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data
    file_name = data["name"]
    bucket_name = data["bucket"]

    print(f"Triggered for file: {file_name} in bucket: {bucket_name}")

    # Initialize storage client and bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Define the expected columns
    expected_columns = ["id", "name", "date", "swipe_in", "swipe_out"]

    try:
        print("Validating file name...")
        validate_file_name(file_name)

        print("Validating file content...")
        data_frame = validate_file_empty(bucket, file_name)

        print("Validating columns...")
        validate_columns(data_frame, expected_columns)

        print("Validating file size...")
        validate_file_size(bucket, file_name)

        print(f"File {file_name} processed successfully.")

        # Trigger DAG
        file_info = {
            "bucket": bucket_name,
            "name": file_name,
            "metageneration": data["metageneration"],
            "timeCreated": data["timeCreated"],
            "updated": data["updated"],
        }

        response = trigger_dag(WEB_SERVER_URL, DAG_ID, file_info)
        print(f"DAG triggered successfully: {response}")

    except Exception as e:
        failure_reason = str(e)
        send_email_via_gmail(failure_reason, file_name)
        print(f"Failure email sent for {file_name}: {failure_reason}")
