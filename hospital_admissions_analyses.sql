USE [healthcare_admissions]
GO

SELECT COUNT(DISTINCT Hospital_canonical) AS [Number of Hospitals] FROM admissions_1;
GO

-- SAFE_CAST equivalent — TRY_CAST in SQL Server
SELECT
    Name,
    Medical_Condition,
    Billing_Amount                          AS raw_billing,
    TRY_CAST(Billing_Amount AS FLOAT)       AS safe_billing,
    CASE
        WHEN TRY_CAST(Billing_Amount AS FLOAT) IS NULL
        THEN 'CONVERSION FAILED - investigate source row'
        ELSE 'Valid'
    END                                     AS billing_data_quality_flag,
    Admission_Date_clean,
    Discharge_Date_clean
FROM admissions_1
WHERE TRY_CAST(Billing_Amount AS FLOAT) IS NULL
   OR Billing_Amount IS NULL;
GO

SELECT TOP 5 
    Billing_Amount,
    TRY_CAST(Billing_Amount AS FLOAT) AS safe_billing
FROM admissions_1;
GO

-- Hospital Stay Analyses

-- Average length of stay across all admissions
SELECT
    AVG(DATEDIFF(DAY, Admission_Date_clean, Discharge_Date_clean)) AS avg_stay
FROM admissions_1;
GO

-- Top 100 longest stays
SELECT TOP 100
    Name,
    Medical_Condition,
    Hospital_canonical,
    DATEDIFF(DAY, Admission_Date_clean, Discharge_Date_clean) AS stay_length
FROM admissions_1
ORDER BY stay_length DESC;
GO

-- monthly admissions with billing and abnormal results (2024)
SELECT
    YEAR(Admission_Date_clean)                                     AS year,
    MONTH(Admission_Date_clean)                                    AS month,
    COUNT(*)                                                    AS admissions,
    ROUND(AVG(Billing_Amount), 2)                               AS avg_billing,
    SUM(CASE WHEN Test_Results = 'Abnormal' THEN 1 ELSE 0 END)  AS abnormal_results
FROM admissions_1
WHERE Admission_Date_clean BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY YEAR(Admission_Date_clean), MONTH(Admission_Date_clean)
ORDER BY year, month;
GO

-- Gender condition ranking with percentage share
WITH gender_condition_counts AS (
    SELECT
        Gender,
        Medical_Condition,
        COUNT(*) AS total_cases
    FROM admissions_1
    GROUP BY Gender, Medical_Condition
),

gender_condition_ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY Gender
            ORDER BY total_cases DESC
        ) AS condition_rank,
        ROUND(
            CAST(total_cases AS FLOAT) /
            NULLIF(SUM(total_cases) OVER (PARTITION BY Gender), 0)
            * 100, 2
        ) AS pct_of_gender_admissions
    FROM gender_condition_counts
)

SELECT
    Gender,
    Medical_Condition,
    total_cases,
    condition_rank,
    pct_of_gender_admissions
FROM gender_condition_ranked
WHERE condition_rank <= 3
ORDER BY Gender, condition_rank;
GO

-- Prescribed Medications Analysis
SELECT
    Medical_Condition,
    Medication,
    COUNT(*)                                                        AS prescription_count,
    SUM(CASE WHEN Gender = 'Female' THEN 1 ELSE 0 END)             AS female_prescriptions,
    ROUND(
        CAST(SUM(CASE WHEN Gender = 'Female' THEN 1 ELSE 0 END) AS FLOAT)
        / NULLIF(COUNT(*), 0) * 100
    , 2)                                                            AS female_percentage
FROM admissions_1
GROUP BY Medical_Condition, Medication
ORDER BY Medical_Condition, prescription_count DESC;
GO

-- Medical Conditions by Hospital: summary with billing and stay
SELECT
    Hospital_canonical,
    Medical_Condition,
    COUNT(*)                                                    AS total_cases,
    ROUND(AVG(TRY_CAST(Billing_Amount AS FLOAT)), 2)            AS avg_billing,
    ROUND(AVG(CAST(DATEDIFF(DAY, Admission_Date_clean, Discharge_Date_clean) AS FLOAT)), 1) AS avg_stay_days
FROM admissions_1
WHERE Hospital_canonical IS NOT NULL
AND Medical_Condition IS NOT NULL
GROUP BY Hospital_canonical, Medical_Condition
ORDER BY Hospital_canonical, total_cases DESC;
GO

-- Top 3 conditions per hospital using CTE ranking
WITH condition_counts AS (
    SELECT
        Hospital_canonical,
        Medical_Condition,
        COUNT(*) AS cases
    FROM admissions_1
    WHERE Hospital_canonical IS NOT NULL
    GROUP BY Hospital_canonical, Medical_Condition
),

condition_ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY Hospital_canonical
            ORDER BY cases DESC
        ) AS condition_rank
    FROM condition_counts
)

SELECT
    Hospital_canonical,
    Medical_Condition,
    cases,
    condition_rank
FROM condition_ranked
WHERE condition_rank <= 3
ORDER BY Hospital_canonical, condition_rank;
GO

-- Cost analysis by condition and hospital
SELECT
    Medical_Condition,
    Hospital_canonical,
    ROUND(AVG(TRY_CAST(Billing_Amount AS FLOAT)), 2)                AS avg_total_bill,
    ROUND(AVG(
        CAST(Billing_Amount AS FLOAT) /
        NULLIF(DATEDIFF(DAY, Admission_Date_clean, Discharge_Date_clean), 0)
    ), 2)                                                           AS avg_cost_per_day,
    SUM(CASE
        WHEN DATEDIFF(DAY, Admission_Date_clean, Discharge_Date_clean) = 0
        THEN 1 ELSE 0
    END)                                                            AS zero_stay_anomalies
FROM admissions_1
GROUP BY Medical_Condition, Hospital_canonical
ORDER BY avg_cost_per_day DESC;
GO

--Which hospital has the highest number of cases
SELECT TOP 5
    Hospital_canonical,
    COUNT(*) AS cases
FROM admissions_1
GROUP BY Hospital_canonical
ORDER BY cases DESC;
GO

-- Part 3: Condition and admission type frequency for Smith hospital
SELECT
    Medical_Condition,
    Admission_Type,
    COUNT(*) AS frequency
FROM admissions_1
WHERE Hospital_canonical = 'LLC Smith'
GROUP BY Medical_Condition, Admission_Type
ORDER BY Admission_Type, frequency DESC;
GO
-- Test results breakdown by admission type with percentage of total
SELECT
    Admission_Type,
    Test_Results,
    COUNT(*) AS total,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()
    , 2) AS percentage_of_total
FROM admissions_1
GROUP BY Admission_Type, Test_Results
ORDER BY Test_Results, total DESC;
GO

-- Admission type cost banding
SELECT
    Admission_Type,
    CASE
        WHEN TRY_CAST(Billing_Amount AS FLOAT) > 40000      THEN 'High Cost'
        WHEN TRY_CAST(Billing_Amount AS FLOAT) BETWEEN 20000
             AND 40000                                       THEN 'Medium Cost'
        ELSE 'Low Cost'
    END AS cost_category,
    COUNT(*) AS cases
FROM admissions_1
GROUP BY Admission_Type,
    CASE
        WHEN TRY_CAST(Billing_Amount AS FLOAT) > 40000      THEN 'High Cost'
        WHEN TRY_CAST(Billing_Amount AS FLOAT) BETWEEN 20000
             AND 40000                                       THEN 'Medium Cost'
        ELSE 'Low Cost'
    END
ORDER BY Admission_Type;
GO


-- What proportion of admissions require further medical attention?
SELECT
    CASE
        WHEN Test_Results = 'Abnormal' THEN 'Requires Attention'
        WHEN Test_Results = 'Normal'   THEN 'Stable'
        ELSE 'Inconclusive'
    END AS result_category,
    COUNT(*) AS total_cases
FROM admissions_1
GROUP BY
    CASE
        WHEN Test_Results = 'Abnormal' THEN 'Requires Attention'
        WHEN Test_Results = 'Normal'   THEN 'Stable'
        ELSE 'Inconclusive'
    END;
GO

-- Male vs female admission counts
SELECT 'Male'   AS gender_group, COUNT(*) AS admissions
FROM admissions_1 WHERE Gender = 'Male'
UNION ALL
SELECT 'Female', COUNT(*)
FROM admissions_1 WHERE Gender = 'Female';
GO

-- Emergency vs elective admissions
SELECT 'Emergency Admissions' AS category, COUNT(*) AS total
FROM admissions_1 WHERE Admission_Type = 'Emergency'
UNION ALL
SELECT 'Elective Admissions', COUNT(*)
FROM admissions_1 WHERE Admission_Type = 'Elective';
GO

-- Average billing vs average daily treatment cost
SELECT 'Average Billing' AS metric,
    AVG(TRY_CAST(Billing_Amount AS FLOAT)) AS value
FROM admissions_1
UNION ALL
SELECT 'Average Cost Per Day',
    AVG(
        TRY_CAST(Billing_Amount AS FLOAT) /
        NULLIF(DATEDIFF(DAY, Admission_Date_clean, Discharge_Date_clean), 0)
    )
FROM admissions_1;
GO

