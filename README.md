# NeoStats: End-to-End Sales Data Processing for Retail Optimization

This project demonstrates an end-to-end data engineering pipeline for processing, analyzing, and visualizing retail sales data. The pipeline is designed to handle data ingestion, cleaning, transformation, and visualization using Microsoft Azure services and Power BI.

---

## Table of Contents
- [Project Overview](#project-overview)
- [Technologies Used](#technologies-used)
- [Features](#features)
- [Data Pipeline Workflow](#data-pipeline-workflow)
- [Insights Delivered](#insights-delivered)
- [Setup Instructions](#setup-instructions)
- [Key Learnings](#key-learnings)
- [Acknowledgments](#acknowledgments)

---

## Project Overview
NeoStats is a retail data processing solution built to optimize sales data handling and provide actionable insights into business performance. It addresses challenges in:
- Sales trend analysis.
- Product and customer segmentation.
- Real-time dashboard generation for key business metrics.

---

## Technologies Used
- **Cloud Platform**: Microsoft Azure
- **Storage**: Azure Data Lake Gen2
- **Data Ingestion**: Azure Data Factory
- **Data Processing**: Azure Databricks and PySpark
- **Visualization**: Power BI
- **Programming Language**: Python (PySpark)

---

## Features
- **Automated Data Ingestion**: Seamless integration of raw datasets into Azure Data Lake.
- **Scalable Data Processing**: PySpark for handling large volumes of sales data.
- **Data Security**: Sensitive data anonymization using SHA-256 and masking techniques.
- **Real-Time Dashboards**: Interactive visualization of sales and customer trends using Power BI.

---

## Data Pipeline Workflow
1. **Data Ingestion**:
   - Raw sales data is ingested into Azure Data Lake Gen2 using Azure Data Factory.
   - Files are organized into `raw_data` and `processed_data` containers.
2. **Data Processing**:
   - Cleaning and transformation are performed in Azure Databricks using PySpark.
   - Key transformations include:
     - Data anonymization for PII fields.
     - Calculation of sales metrics (e.g., Total Sales, Average Order Value).
   - Processed data is written back to Azure Data Lake.
3. **Visualization**:
   - Power BI dashboards connect to the processed data in Azure Data Lake for real-time insights.

---

## Insights Delivered
1. **Sales Performance**:
   - Total Sales and Average Sales by year, region, and product category.
2. **Customer Segmentation**:
   - High-value customers based on purchase frequency and total spend.
3. **Product Analysis**:
   - Best-selling products and their contribution to overall sales.
4. **Geographical Trends**:
   - Regional analysis of sales trends.

---

## Setup Instructions

### Azure Resource Setup
1. **Create a Resource Group**:
   - Set up a resource group in the Azure Portal.
2. **Storage Account**:
   - Create an Azure Data Lake Gen2 account.
   - Enable Hierarchical Namespace.
   - Create containers for `raw_data` and `processed_data`.
3. **Data Factory**:
   - Create pipelines to ingest raw sales data into the storage account.
4. **Databricks Workspace**:
   - Set up a Databricks workspace and cluster.
   - Use the following script to mount Azure Data Lake:
     ```python
     configs = {
         "fs.azure.account.auth.type": "OAuth",
         "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
         "fs.azure.account.oauth2.client.id": "<CLIENT_ID>",
         "fs.azure.account.oauth2.client.secret": "<CLIENT_SECRET>",
         "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<TENANT_ID>/oauth2/token"
     }

     dbutils.fs.mount(
         source="abfss://<CONTAINER_NAME>@<STORAGE_ACCOUNT>.dfs.core.windows.net",
         mount_point="/mnt/retail_data",
         extra_configs=configs
     )
     ```
5. **Power BI**:
   - Connect Power BI to the processed data in Azure Data Lake.
   - Create dashboards for insights.

---

## Key Learnings
- Implementing scalable data pipelines for large-scale retail data processing.
- Using PySpark for efficient data transformation.
- Designing secure pipelines with data anonymization and masking.
- Creating insightful dashboards to support business decisions.

---

