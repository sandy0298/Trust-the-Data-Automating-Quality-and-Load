# Trust-the-Data-Automating-Quality-and-Load

# Data Processing Workflow 🚀

## Overview

This repository outlines an end-to-end **Data Processing Workflow** built on **Google Cloud Platform (GCP)**. The process begins with files arriving in a **Google Cloud Storage (GCS)** bucket and continues through a series of validation and transformation steps, ending with a success notification.

---
## Architecture Diagram

<img src ="https://github.com/sandy0298/Trust-the-Data-Automating-Quality-and-Load/blob/main/images/automation.jpg" width="800" height="600" alt="architecture"/> &emsp;

## 🔁 Workflow Steps

1. **📁 Files Arrive in GCS**  
   Data files are uploaded or ingested into a designated Google Cloud Storage bucket.

2. **✅ Data Quality Checks**  
   Initial validation of data integrity, format, and completeness.
   - **If valid**: Continue to data pipeline.
   - **If invalid**: Trigger failure notification.

3. **🎯 Trigger Composer DAG**  
   An **Apache Airflow** DAG (hosted in Cloud Composer) is triggered to orchestrate the data pipeline.

4. **⚙️ Create Dataproc Cluster**  
   A temporary **Dataproc** cluster is created for distributed data processing using **Apache Spark**.

5. **🐍 Submit PySpark Job**  
   A PySpark job is submitted to transform, clean, and prepare the data.

6. **🧠 Write to BigQuery**  
   Processed data is loaded into **BigQuery** for analytics and reporting.

7. **🗃️ Move Data to Archival Folder**  
   Original raw files are moved from the staging area to an archival folder in GCS for future reference or audit.

8. **❌ Delete Dataproc Cluster**  
   The temporary Dataproc cluster is deleted to avoid unnecessary costs.

9. **📧 Send Success Email**  
   A success notification email is sent to stakeholders confirming the pipeline completed successfully.

---

## ❌ Failure Handling

- If the data fails the quality checks, the pipeline halts and a **failure email** is sent to notify the appropriate team.

---

## Flowchart Diagram

<img src ="https://github.com/sandy0298/Trust-the-Data-Automating-Quality-and-Load/blob/main/images/flowchart.png" width="800" height="900" alt="architecture"/> &emsp;


## 📦 Technologies Used

- Google Cloud Storage (GCS)
- Cloud Composer (Apache Airflow)
- Google Cloud Dataproc
- Apache Spark (PySpark)
- Google BigQuery
- Cloud Functions / Pub/Sub (trigger mechanism)
- Email Notifications (SendGrid / Cloud Functions)

---

## 📬 Notifications

All pipeline executions end with a notification:
- ✅ **Success**: Process completed and data is available in BigQuery.
- ❌ **Failure**: Data validation failed or job execution errored out.

---

## 📫 Contact

Feel free to open an issue or connect via [[LinkedIn](https://www.linkedin.com/in/sandeep-mohanty-b25418172/)] for collaboration or queries.

