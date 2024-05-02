-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Cleaning and Preparation

-- COMMAND ----------

-- Drop and create clinicaltrial_2020
DROP TABLE IF EXISTS clinicaltrial_2020;
CREATE TABLE clinicaltrial_2020
USING CSV
OPTIONS (
  header='true',
  delimiter='|',
  inferSchema='true',
  mode='FAILFAST',
  path='/FileStore/tables/clinicaltrial_2020.csv'
);

-- Cache clinicaltrial_2020
CACHE TABLE clinicaltrial_2020;

-- COMMAND ----------

-- Drop and create clinicaltrial_2021
DROP TABLE IF EXISTS clinicaltrial_2021;
CREATE TABLE clinicaltrial_2021
USING CSV
OPTIONS (
  header='true',
  delimiter='|',
  inferSchema='true',
  mode='FAILFAST',
  path='/FileStore/tables/clinicaltrial_2021.csv'
);

-- Cache clinicaltrial_2021
CACHE TABLE clinicaltrial_2021;

-- COMMAND ----------

-- Drop and create clinicaltrial_2021
DROP TABLE IF EXISTS clinicaltrial_2023;
CREATE TABLE clinicaltrial_2023
USING CSV
OPTIONS (
  header='true',
  inferSchema='true',
  mode='FAILFAST',
  path='/FileStore/tables/clinicaltrial1_2023.csv/'
);

-- Cache clinicaltrial_2023
CACHE TABLE clinicaltrial_2023;

-- COMMAND ----------

-- Drop and create Pharma
DROP TABLE IF EXISTS pharma;

CREATE TABLE pharma
USING CSV
OPTIONS (
  header='true',
  delimiter=',',
  inferSchema='true', -- Specify 'true' to infer schema
  mode='PERMISSIVE',
  path='/FileStore/tables/pharma.csv'
);

-- Cache pharma
CACHE TABLE pharma;


-- COMMAND ----------

-- Display the list of tables
SHOW TABLES;

-- COMMAND ----------

-- Show the first few rows of the clinicaltrial_2023 table
SELECT * FROM clinicaltrial_2023

-- COMMAND ----------

-- Show the first few rows of the pharma table
SELECT * FROM pharma

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## PROBLEM ANSWERS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##QUESTION 1
-- MAGIC The number of studies in the dataset.

-- COMMAND ----------

SELECT COUNT(*) FROM clinicaltrial_2023;

-- COMMAND ----------

SELECT COUNT(DISTINCT `Id`) AS distinct_study_count FROM clinicaltrial_2023;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## QUESTION 2
-- MAGIC List all the types (as contained in the Type column) of studies in the dataset along with the frequencies of each type. These should be ordered from most frequent to least frequent.

-- COMMAND ----------

--  All types of studies along with their frequencies
SELECT
  Type,
  COUNT(Type) AS frequency
FROM
  clinicaltrial_2023
WHERE
  Type IS NOT NULL
GROUP BY
  Type
ORDER BY
  frequency DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## QUESTION 3
-- MAGIC The top 5 conditions (from Conditions) with their frequencies

-- COMMAND ----------

-- Select the separate conditions and their frequencies in clinicaltrial_2023
SELECT
  Conditons AS Conditions,
  COUNT(*) AS Frequency
FROM (
  SELECT
    explode(split(Conditions, '[|]')) AS Conditons
  FROM
    clinicaltrial_2023
  WHERE
    Conditions IS NOT NULL AND Conditions != ''
)
GROUP BY
  Conditons
ORDER BY
  Frequency DESC
LIMIT 5;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## QUESTION 4
-- MAGIC 10 most common sponsors that are not pharmaceutical companies + the number of clinical trials sponsored.

-- COMMAND ----------

-- Create a view to show the sponsors
CREATE OR REPLACE TEMPORARY VIEW SPONSORS AS
SELECT Sponsor
FROM clinicaltrial_2023;

-- Select from the SPONSORS view
SELECT * FROM SPONSORS;

-- COMMAND ----------

-- Create a view for the pharmaceutical companies
CREATE OR REPLACE TEMPORARY VIEW pharmaceutical AS
SELECT Parent_Company
FROM pharma;

-- Select from the pharmaceutical view
SELECT * FROM pharmaceutical;

-- COMMAND ----------

-- Create a view for sponsors that are not pharmaceutical companies
CREATE OR REPLACE TEMPORARY VIEW Sponsors_not_pharmaceuticals AS
SELECT *
FROM SPONSORS
WHERE Sponsor NOT IN (SELECT Parent_Company FROM pharmaceutical);

-- Select from the Sponsors_not_pharmaceuticals view
SELECT * FROM Sponsors_not_pharmaceuticals;

-- COMMAND ----------

-- 10 most common sponsors that are not pharmaceuticals
SELECT Sponsor, COUNT(*) AS Count
FROM Sponsors_not_pharmaceuticals
WHERE Sponsor IS NOT NULL
GROUP BY Sponsor
ORDER BY Count DESC
LIMIT 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## QUESTION 5
-- MAGIC Creating Table and Visualization for Completed Studies

-- COMMAND ----------

-- Number of completed studies per month in 2023
SELECT SUBSTRING(Completion, 6, 2) AS Month, COUNT(*) AS No_Of_Completed_Studies
FROM clinicaltrial_2023
WHERE Status = 'COMPLETED' AND Completion IS NOT NULL AND YEAR(Completion) = 2023
GROUP BY SUBSTRING(Completion, 6, 2)
ORDER BY No_Of_Completed_Studies DESC;

-- COMMAND ----------

-- Number of completed studies per month in 2023
SELECT SUBSTRING(Completion, 6, 2) AS Month, COUNT(*) AS No_Of_Completed_Studies
FROM clinicaltrial_2023
WHERE Status = 'COMPLETED' AND Completion IS NOT NULL AND YEAR(Completion) = 2023
GROUP BY SUBSTRING(Completion, 6, 2)
ORDER BY No_Of_Completed_Studies DESC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## EXTRA MARK
-- MAGIC Comparison between the number of completed studies for 2020, 2021 and 2023

-- COMMAND ----------

-- Number of Completed Studies in 2023 grouped by year
CREATE OR REPLACE TEMP VIEW Completed_Studies_2023 AS
SELECT SUBSTRING(Completion, 1, 4) AS Year, COUNT(*) AS Study_Count
FROM clinicaltrial_2023
WHERE Status = 'COMPLETED' AND Completion IS NOT NULL
AND SUBSTRING(Completion, 1, 4) = '2023'
GROUP BY SUBSTRING(Completion, 1, 4);

-- Display the result
SELECT * FROM Completed_Studies_2023;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Visualization of the comparison of the number of Completed Studies between 2020, 2021, 2023.

-- COMMAND ----------

-- Define the data for completed studies
CREATE OR REPLACE TEMP VIEW Completed_Studies AS
SELECT 'Year_2020' AS Year, 18615 AS Study_Count
UNION ALL
SELECT 'Year_2021' AS Year, 18689 AS Study_Count
UNION ALL
SELECT 'Year_2023' AS Year, 15467 AS Study_Count;

-- Display the result
SELECT * FROM Completed_Studies;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Counting the Status of the Year 2023

-- COMMAND ----------

SELECT status, COUNT(*) AS count
FROM clinicaltrial_2023
GROUP BY status;

