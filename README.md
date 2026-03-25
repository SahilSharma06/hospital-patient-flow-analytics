#hospital patient flow analytics

An end-to-end data engineering solution designed to track and analyze patient flow across a hospital network in real-time. [cite_start]It leverages a Medallion Architecture to move data from raw ingestion to a highly optimized Gold layer for reporting.

System Architecture:
1. Data Generation & Ingestion
> Source: A Python-based simulation script generates real-time patient events (admissions, discharges, etc.).
> Ingestion: Data is streamed to **Azure Event Hubs** using a Kafka producer.
> Bronze Layer: Raw JSON data is captured from the stream and persisted into **Delta Lake** tables on ADLS Gen2.

2. Data Transformation (Medallion Architecture)
Silver Layer (Cleaning):
> Validates schemas and parses raw JSON.
> Handles "dirty" data, such as invalid ages or future admission timestamps.
> Standardizes data types for timestamps and IDs.
3. Gold Layer (Business Logic):
> SCD Type 2: Implements Slowly Changing Dimensions for the `dim_patient` table to track historical changes.
> Fact Modeling: Generates a `fact_patient_flow` table with calculated metrics like `length_of_stay_hours`.

3. Serving & Visualization
> Azure Synapse: External tables and views are created over the Gold Parquet files to provide a SQL-based serving layer.
> KPI Views: Pre-calculated SQL views for Bed Occupancy, Turnover Rate, and Patient Demographics.
>Power BI: A specialized dashboard connects to the Synapse SQL pool to visualize volume trends and overstay alerts.
