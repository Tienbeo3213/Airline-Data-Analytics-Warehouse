# Airline-Data-Analytics-Warehouse using Hadoop, Hive, Spark and Tableau
 Airline data analysis using Hadoop, Hive, Spark, and Tableau to optimize performance, predict delays, and enhance passenger experience.
Project Overview
This project explores the potential of Big Data technologies to analyze airline data effectively. The aim is to provide actionable insights into flight performance, delays, and cancellations while enhancing customer experience and operational efficiency in the aviation sector.

## Features
Data Management: Use of Hadoop HDFS for distributed storage and Hive for structured querying.
Data Processing: Apache Spark for ETL (Extract, Transform, Load) processes and data cleaning.
Data Visualization: Tableau for creating interactive dashboards and insightful visual reports.
Pipeline Workflow: Automated steps for ingestion, transformation, and querying of datasets.
## Dataset
The project uses publicly available datasets related to airports, airlines, and flights:

airports.csv - Information about airports (location, IATA codes, etc.).
carriers.csv - Airline information.
detailed_data.csv - Flight details (departure/arrival times, delays, etc.).
plane-data.csv - Aircraft specifications and details.
## Key Technologies
Hadoop: For large-scale distributed data storage.
Hive: To create and query databases using SQL-like syntax.
Apache Spark: For scalable and fast data processing.
Tableau: For visualization and creating dashboards.

## Project Workflow
Data Ingestion:
Use hdfs_put_data.sh to upload datasets into HDFS.
Database Creation:
Create databases in Hive for raw and processed data.
Data Processing:
Use PySpark scripts for ETL processes:
Cleaning and transforming raw data.
Aggregating flight performance metrics.
Data Analysis:
Use Hive queries to generate insights about delays, cancellations, and flight performance.
Visualization:
Import processed data into Tableau to create interactive dashboards.
