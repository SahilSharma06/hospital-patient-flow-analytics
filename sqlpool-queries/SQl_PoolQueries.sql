Create master key ENCRYPTION by PASSWORD = '<<Password>>';

create database hospital_analytics_db;

-- CREATING A SCOPE
CREATE DATABASE SCOPED CREDENTIAL storage_credential
WITH IDENTITY = 'Managed Identity';

-- DEFINING DATA SOURCE
CREATE EXTERNAL DATA SOURCE gold_data_source
WITH (
    LOCATION = "abfss://<<Container>>.<<StorageAccountName>>.dfs.core.windows.net"
,
    CREDENTIAL = storage_credential
);

-- Patient Dim View
CREATE OR ALTER VIEW dbo.dim_patient AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_patient',
    DATA_SOURCE = 'gold_data_source',
    FORMAT = 'DELTA'
) AS rows;

-- Department View
CREATE OR ALTER VIEW dbo.dim_department AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_department',
    DATA_SOURCE = 'gold_data_source',
    FORMAT = 'DELTA'
) AS rows;

-- fact_view
CREATE OR ALTER VIEW dbo.fact_patient_flow AS
SELECT *
FROM OPENROWSET(
    BULK 'fact_patient_flow',
    DATA_SOURCE = 'gold_data_source',
    FORMAT = 'DELTA'
) AS rows;


SELECT TOP 10 * FROM dbo.dim_patient;
SELECT TOP 10 * FROM dbo.dim_department;
SELECT TOP 10 * FROM dbo.fact_patient_flow;

SELECT
    department_sk,
    COUNT(*) AS total_admissions,
    AVG(length_of_stay_hours) AS avg_los
FROM dbo.fact_patient_flow
GROUP BY department_sk;

SELECT name, type_desc
FROM sys.objects
WHERE type = 'V';

SELECT COUNT(*) FROM dbo.dim_patient;

SELECT COUNT(*) FROM dbo.fact_patient_flow;

SELECT COUNT(*) FROM dbo.dim_department;

SELECT TOP 20
    f.patient_sk,
    p.patient_id,
    d.department,
    f.admission_time,
    f.length_of_stay_hours
FROM dbo.fact_patient_flow f
LEFT JOIN dbo.dim_patient p
    ON f.patient_sk = p.surrogate_key
LEFT JOIN dbo.dim_department d
    ON f.department_sk = d.surrogate_key;


SELECT 
    d.department,
    COUNT(*) AS total_admissions,
    AVG(f.length_of_stay_hours) AS avg_stay_hours
FROM dbo.fact_patient_flow f
JOIN dbo.dim_department d
    ON f.department_sk = d.surrogate_key
GROUP BY d.department
ORDER BY total_admissions DESC;




