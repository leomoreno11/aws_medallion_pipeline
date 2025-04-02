# **Project Overview**  
This project implements an end-to-end data pipeline using Airflow to extract data from the [Open Brewery DB API](https://api.openbrewerydb.org/breweries) and store it in a Snowflake-based data lake. The pipeline follows the **medallion architecture** (bronze, silver, and gold layers) and automates data ingestion, transformation, and aggregation for analytical insights.  

### **Objective**  
The goal is to showcase scalable, cloud-native data processing while ensuring data quality through validation checks and automated monitoring.  

![image](https://private-user-images.githubusercontent.com/67594083/430185325-7d420b77-44a7-41ad-8a5f-317fe2fac5f1.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDM3Mjg0MjcsIm5iZiI6MTc0MzcyODEyNywicGF0aCI6Ii82NzU5NDA4My80MzAxODUzMjUtN2Q0MjBiNzctNDRhNy00MWFkLThhNWYtMzE3ZmUyZmFjNWYxLnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNTA0MDQlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjUwNDA0VDAwNTUyN1omWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPWUxNThjYWQ1Y2Q4OWY2Zjc3N2QxOWExZTQ5NTc3ZWMxNDg2NGEwNmRiMDQzZTAyMzQ1NDU3MDZjMWZkNTRiMzkmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0In0.In06qlQnf0R304ckPAsybRgsM-Yfsf8zH1NQV0kGCXM)  


---

## **Requirements**  
To deploy and run this project, you need:  
- 🌬️ **Airflow**: Orchestrates the workflow.  
- 🪣 **AWS S3**: Stores raw data and queries.  
- 🐳 **Docker**: Containerizes the environment.  
- 🌿 **Git**: Manages version control.  
- ❄️ **Snowflake**: Serves as the data lake.  
- 🐍 **Python 3.11**: Runs Airflow tasks.  

---

## **Project Structure**  
The pipeline follows a **three-layer architecture**:  
- 🥉 **Bronze Layer**: Stores raw API responses in JSON format.  
- 🥈 **Silver Layer**: Cleans, transforms, and partitions data in Snowflake.  
- 🥇 **Gold Layer**: Generates aggregated brewery data by type and location.  

---

## **Solution Workflow**  
1. **Extract Data**: Fetch data from the API and store it in S3 as raw JSON.  
2. **Load into Snowflake**: Ingest raw data into the bronze layer.  
3. **Transform Data**: Clean and format data in the silver layer, partitioning it by location.  
4. **Aggregate Insights**: Generate summary views in the gold layer for analysis.  
5. **Validate Data**: Ensure data consistency with record validation and retry logic.  
6. **Monitor & Alert**: Detect failures and send Slack notifications.  

---

## **Setting Up Cloud Services & Alerts**  

### **Amazon S3 Setup**  
1. **Create an AWS Account**: Sign up at [AWS](https://aws.amazon.com/).  
2. **Generate AWS Credentials**: Use the [IAM Console](https://console.aws.amazon.com/iam/) to create access keys.  
3. **Set Up an S3 Bucket**:  
   - Create a bucket and configure paths for API responses and query storage.  
   - Use dedicated user accounts and configure permissions to restrict access.  

#### **Required Airflow Variables & Connections**  
- **Airflow Variables**:  
  - `airflow_s3_bucket`: Your S3 bucket name.  
  - `open_brewery_db_api_response_path`: Path for storing JSON responses.  
  - `open_brewery_db_query_path`: Path for pipeline queries.  
- **Airflow Connection**:  
  - `s3_conn`: Set up an "Amazon Web Services" connection with your AWS credentials.  

---

### **Snowflake Setup**  
1. **Create a Snowflake Account**: Register at [Snowflake](https://www.snowflake.com/).  
2. **Initialize the Database**:  
   - Run SQL scripts in `/project_assets/queries/datalake_environment_set_up/`.  
   - Start with `setting_up_snowflake_environment.sql` to configure `WAREHOUSE`, `DATABASE`, `SCHEMAS`, `TABLES`, and `ROLES`.  
   - Run `setting_up_pipeline_elements.sql` to enable pipeline-related resources.  

#### **Required Airflow Variables & Connections**  
- **Airflow Variables**:  
  - `snowflake_database`: The database name used in Snowflake.  
- **Airflow Connection**:  
  - `snowflake_conn`: Configure a connection of type "Snowflake" fill the fields q `Login`, `Password`, `Warehouse`, `Account`, `Region`, and `Role`. You can leave the value `insecure_mode` in the bottom of the page checked.

---

### **Slack Integration**  
1. **Create a Slack App**: Go to [Slack API](https://api.slack.com/apps) and create an app.  
2. **Set Up Bot Permissions**: Add `chat:write`, `app_mentions:read`, and other required permissions.  
3. **Install the App**: Authorize the app in your Slack workspace.  
4. **Obtain API Token**: Securely store the `Bot User OAuth Access Token`.  

#### **Required Airflow Variable**  
- `slack_secret_token`: The `Bot User OAuth Access Token` for Slack API integration.  

---

## **Deploying the Project**  

### **1. Clone the Repository**  
```bash
git clone <repository-url>
cd <repository-name>
```  

### **2. Start Docker Containers**  
Ensure Docker is installed in your machine, then start the necessary services:  
```bash
docker-compose up -d
```  

### **3. Verify Cloud Configuration**  
- Confirm that S3, Snowflake, and Slack are correctly set up.  

### **4. Run Airflow**  
- Access the Airflow UI at `http://localhost:9090`.  
- Trigger the DAG manually or let it run on schedule.  

---

## **Running the Pipeline**  
The Airflow DAG runs **daily at midnight** but can be manually triggered via the UI.  

![image](https://private-user-images.githubusercontent.com/67594083/430185520-cb61aeba-3e99-4c74-bcdc-1e7a9c40d4b2.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDM3Mjg0MjcsIm5iZiI6MTc0MzcyODEyNywicGF0aCI6Ii82NzU5NDA4My80MzAxODU1MjAtY2I2MWFlYmEtM2U5OS00Yzc0LWJjZGMtMWU3YTljNDBkNGIyLnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNTA0MDQlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjUwNDA0VDAwNTUyN1omWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPWFiYmEzNDM2OGMzZTg0ZjljNzY4YWU5N2Y5Zjc5MTg2YjYzYmFlNmMxNTg2YmI5N2U0OWQzYmZkYjFiYTBhMWYmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0In0.0I6auMK7MJ0bZSbIA0vefF0qLErCpn9P-fIbxCKsxXk) 

### **DAG Execution**  
The primary DAG (`OPEN_BREWERY_DB_DATA_PIPELINE`) automates:  
- 🥉 **Raw Data Extraction** (Bronze Layer)  
- 🥈 **Data Transformation** (Silver Layer)  
- 🥇 **Aggregated Insights** (Gold Layer)  

If any task fails, an automatic Slack notification is triggered for immediate resolution.  

![image](https://private-user-images.githubusercontent.com/67594083/430185510-5e2e2131-cb39-4718-8164-41f8dd435947.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NDM3Mjg0MjcsIm5iZiI6MTc0MzcyODEyNywicGF0aCI6Ii82NzU5NDA4My80MzAxODU1MTAtNWUyZTIxMzEtY2IzOS00NzE4LTgxNjQtNDFmOGRkNDM1OTQ3LnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNTA0MDQlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjUwNDA0VDAwNTUyN1omWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPWYxZWY1MjU4YTI3ODY1OTBjYmI2Zjg4MWE0YTY5Mzk0MTY0NTdlNDE0OWM1YTgxOWZiNmFlOGExYzc0NzkyMWMmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0In0.Lf-YG_toS02SdIC8zfOT9q1JCkhB3z1YO6_mrRsgGUg)

### **Execution Options**  
- **Scheduled Runs**: The pipeline runs daily at midnight (configurable via cron).  
- **Manual Runs**: Can be triggered in Airflow for testing or reprocessing.  
