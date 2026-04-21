# Healthcare Admissions SQL Engineering Project

## Overview
A two-layer SQL engineering project built using a synthetic healthcare admissions 
dataset. BigQuery serves as the analytical layer for exploratory analysis, KPI 
reporting and query optimisation. SQL Server 2022 serves as the operational layer 
for debugging scenarios, error handling and stored procedure simulation. The project 
was built to demonstrate production-style SQL investigation, data cleaning and 
performance tuning across two platforms.

## Dataset
55,000 synthetic hospital admission records including patient demographics, medical 
conditions, treatments, billing information and admission types.

## Tools
- Google BigQuery — analytical queries, performance optimisation, data cleaning
- SQL Server 2022 — T-SQL translation, debugging scenarios, stored procedures
- Docker — local SQL Server environment on macOS
- Python — data preprocessing and hospital name entity resolution

## Key Analyses
- Hospital stay patterns and length of stay distributions
- Medical conditions by gender with percentage share
- Medication usage by condition and gender breakdown
- Hospital-level condition analysis and cost per day
- Test results by admission type
- Billing comparisons across admission types and conditions

## Debugging Scenarios
Eight end-to-end T-SQL debugging scenarios simulating real production failures, each 
following an investigate, diagnose, fix and log pattern:
- Conversion errors during staged data ingestion using TRY_CAST and staging tables
- Stored procedure failure logging with TRY_CATCH and a central error log table
- Divide-by-zero protection using NULLIF and defensive stored procedures
- Constraint violation handling during bulk insert operations
- Missing index identification using sys.dm_db_missing_index_details and SET STATISTICS IO
- Implicit conversion diagnosis using execution plan analysis
- Recursive CTE termination debugging with MAXRECURSION
- Parameter sniffing resolution using OPTION(RECOMPILE) and OPTIMIZE FOR UNKNOWN

## Data Cleaning
Hospital names in the raw dataset contained legal suffixes and conjunctions such as 
Ltd, PLC, Group and Sons, causing the same hospital to appear as multiple distinct 
entities in grouping queries. A frequency-weighted anchor token approach was applied 
across 55,000 records in Python — tokenising each name, removing stopwords, and 
selecting the least globally frequent token as the canonical entity identifier. 
TF-IDF cosine similarity was evaluated first but produced a computationally 
infeasible similarity matrix of approximately 22GB at this scale. Patient names were 
standardised using INITCAP() in BigQuery. Post-clean validation confirmed no nulls 
were introduced and distinct hospital count reduced as expected.

## Repository Structure
- bigquery_analysis.sql — data cleaning, validation and analytical queries in BigQuery
- tsql_analysis.sql — BigQuery queries translated to T-SQL for SQL Server
- tsql_debugging.sql — 8 end-to-end debugging and error handling scenarios
- hospital_cleaning.py — Python script for hospital name entity resolution

## Dataset Link
https://www.kaggle.com/code/muhammadfurqan0/unlocking-healthcare-trends-data-analysis/input
