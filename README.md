# Data Warehouse using PySpark for Retail Sales Analysis

This project focuses on building a data warehouse solution for retail sales analysis using PySpark, Delta Lake, and AWS Athena. It involves ETL processes to ingest sales, customers, and product data from S3, transform it using PySpark scripts, and store it in staging, landing, and final dimension and fact tables. The data is then queried using Athena to provide insights into customer sales orders and other key metrics.

## Project Overview

- **ETL Process**: The ETL process involves extracting raw sales, customer, and product data from S3, transforming it using PySpark scripts to create staging, landing, and final tables, and loading the transformed data back to S3.
  
- **Data Modeling**: The project implements dimensional modeling techniques to design dimension and fact tables for efficient data analysis. Dimension tables represent entities such as customers and products, while fact tables capture business transactions like sales orders.

- **Delta Lake**: Delta Lake is used for managing data in the data warehouse, providing features such as ACID transactions, versioning, and efficient data management for insert, update, and delete operations.

- **Querying with Athena**: Transformed data stored in S3 is made queryable using Athena. Symlink manifests are used to optimize query performance and provide fast access to the data.

## Repository Structure

