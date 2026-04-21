/* V1
SELECT *
FROM `healthcare-admissions-dataset.hospital_analysis.admissions`
LIMIT 100;


SELECT
COUNT(*) AS total_rows,
COUNTIF(Name IS NULL) AS missing_name,
COUNTIF(`Date of Admission` IS NULL) AS missing_admission_date
FROM `healthcare-admissions-dataset.hospital_analysis.admissions`; */

CREATE OR REPLACE TABLE
  `healthcare-admissions-dataset.hospital_analysis.admissions_clean` AS

WITH hospital_tokens AS (
  SELECT
    *,
    SPLIT(
      REGEXP_REPLACE(Hospital, r'[^A-Za-z]+', ' '), ' '
    ) AS tokens
  FROM `healthcare-admissions-dataset.hospital_analysis.admissions`
)

SELECT
  * EXCEPT (Name, Hospital, tokens),
  INITCAP(Name)   AS Name,
  (
    SELECT token
    FROM UNNEST(tokens) AS token
    WHERE token NOT IN (
      'and', 'And', 'Ltd', 'Inc', 'LLC',
      'PLC', 'Sons', 'Group', ''
    )
    LIMIT 1
  )               AS Hospital
FROM hospital_tokens;

/*CREATE OR REPLACE TABLE `healthcare-admissions-dataset.hospital_analysis.admissions_clean` AS
WITH hospital_tokens AS (
  SELECT
    *,
    SPLIT(REGEXP_REPLACE(Hospital, r'[^A-Za-z]+', ' '), ' ') AS tokens
  FROM `healthcare-admissions-dataset.hospital_analysis.admissions`
)
SELECT
  * EXCEPT(Name, Hospital, tokens),
  INITCAP(Name) AS Name,
  (
    SELECT token
    FROM UNNEST(tokens) AS token
    WHERE token NOT IN ('and', 'And', 'Ltd', 'Inc', 'LLC', 'PLC', 'Sons', 'Group', '')
    LIMIT 1
  ) AS Hospital
FROM hospital_tokens; 

SELECT *
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
LIMIT 100; */


-- Confirm no nulls introduced by token extraction
SELECT
  COUNTIF(Hospital IS NULL) AS null_hospitals_after_clean,
  COUNTIF(Name IS NULL)     AS null_names_after_clean,
  COUNT(*)                  AS total_rows
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`;

-- ============================================================
-- POST-CLEAN VALIDATION
-- ------------------------------------------------------------
-- After building the clean table, verify the fix worked:
--   - Hospital name count should be lower than raw
--   - No NULL hospitals introduced by the cleaning logic
--   - Name casing should be consistent
-- ============================================================

SELECT
  'Raw hospital distinct count'     AS metric,
  COUNT(DISTINCT Hospital)          AS value
FROM `healthcare-admissions-dataset.hospital_analysis.admissions`

UNION ALL

SELECT
  'Clean hospital distinct count',
  COUNT(DISTINCT Hospital)
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`;


SELECT
  Name,
  `Medical Condition`,
  `Billing Amount`                                AS raw_billing,
  SAFE_CAST(`Billing Amount` AS FLOAT64)          AS safe_billing,
  CASE
    WHEN SAFE_CAST(`Billing Amount` AS FLOAT64) IS NULL
    THEN 'CONVERSION FAILED — investigate source row'
    ELSE 'Valid'
  END                                             AS billing_data_quality_flag,
  `Date of Admission`,
  `Discharge Date`
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
WHERE SAFE_CAST(`Billing Amount` AS FLOAT64) IS NULL  -- isolate problem rows
   OR `Billing Amount` IS NULL;


/*Hospital Stay Analyses */
SELECT
AVG(DATE_DIFF(`Discharge Date`, `Date of Admission`,DAY))AS avg_stay
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`;

SELECT
Name,
`Medical Condition`,
Hospital,
DATE_DIFF(`Discharge Date`, `Date of Admission`, DAY) AS stay_length
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
ORDER BY stay_length DESC
LIMIT 100;

/*V1
SELECT
EXTRACT(YEAR FROM `Date of Admission`)AS year,
EXTRACT(MONTH FROM `Date of Admission`)AS month,
COUNT(*)AS admissions
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
GROUP BY year,month
ORDER BY year,month;*/

SELECT
  EXTRACT(YEAR FROM `Date of Admission`)  AS year,
  EXTRACT(MONTH FROM `Date of Admission`) AS month,
  COUNT(*)                                AS admissions,
  ROUND(AVG(`Billing Amount`), 2)         AS avg_billing,
  COUNTIF(`Test Results` = 'Abnormal')    AS abnormal_results
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
WHERE `Date of Admission` BETWEEN '2024-01-01' AND '2024-12-31'  -- partition pruning boundary
GROUP BY year, month
ORDER BY year, month;

/*Gender Analyses */
/*V1
SELECT
Gender,
`Medical Condition`,
COUNT(*) AS total_cases
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
GROUP BY Gender, `Medical Condition`
ORDER BY total_cases DESC;

SELECT *
FROM (
  SELECT
  Gender,
  `Medical Condition`,
  COUNT(*) AS total_cases,
  RANK() OVER(PARTITION BY Gender ORDER BY COUNT(*) DESC) AS condition_rank
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
  GROUP BY Gender, `Medical Condition`
)
WHERE condition_rank <= 3; */

WITH gender_condition_counts AS (
  SELECT
    Gender,
    `Medical Condition`,
    COUNT(*) AS total_cases
  FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
  GROUP BY Gender, `Medical Condition`
),

gender_condition_ranked AS (
  SELECT
    *,
    RANK() OVER (
      PARTITION BY Gender
      ORDER BY total_cases DESC
    ) AS condition_rank,
    ROUND(
      SAFE_DIVIDE(
        total_cases,
        SUM(total_cases) OVER (PARTITION BY Gender)
      ) * 100, 2
    ) AS pct_of_gender_admissions
  FROM gender_condition_counts
)

SELECT
  Gender,
  `Medical Condition`,
  total_cases,
  condition_rank,
  pct_of_gender_admissions
FROM gender_condition_ranked
WHERE condition_rank <= 3
ORDER BY Gender, condition_rank;


/*Prescribed Medications Analyses */
SELECT
  `Medical Condition`,
  Medication,
  COUNT(*) AS prescription_count,
  COUNTIF(Gender = 'Female') AS female_prescriptions,
  ROUND(
    COUNTIF(Gender = 'Female') / COUNT(*) * 100,
    2
  ) AS female_percentage
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
GROUP BY `Medical Condition`, Medication
ORDER BY `Medical Condition`, prescription_count DESC;

/* Medical Conditions by Hospital Analyses */
/* V1
SELECT *
FROM (
  SELECT
    Hospital,
    `Medical Condition`,
    COUNT(*) AS cases,
    RANK() OVER (
      PARTITION BY Hospital
      ORDER BY COUNT(*) DESC
    ) AS condition_rank
  FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
  WHERE Hospital IS NOT NULL
  GROUP BY Hospital, `Medical Condition`
)
WHERE condition_rank <= 3
ORDER BY Hospital; */

SELECT
  Hospital,
  `Medical Condition`,
  COUNT(*)                        AS total_cases,
  ROUND(AVG(`Billing Amount`), 2) AS avg_billing,
  ROUND(AVG(DATE_DIFF(
    `Discharge Date`,
    `Date of Admission`, DAY
  )), 1)                          AS avg_stay_days
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
WHERE Hospital IS NOT NULL
  AND `Medical Condition` IS NOT NULL  -- cluster-aligned filter
GROUP BY Hospital, `Medical Condition`
ORDER BY Hospital, total_cases DESC;

WITH condition_counts AS (
  SELECT
    Hospital,
    `Medical Condition`,
    COUNT(*) AS cases
  FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
  WHERE Hospital IS NOT NULL
  GROUP BY Hospital, `Medical Condition`
),

condition_ranked AS (
  SELECT
    *,
    RANK() OVER (
      PARTITION BY Hospital
      ORDER BY cases DESC
    ) AS condition_rank
  FROM condition_counts
)

SELECT
  Hospital,
  `Medical Condition`,
  cases,
  condition_rank
FROM condition_ranked
WHERE condition_rank <= 3
ORDER BY Hospital, condition_rank;


/* Part 2 - with info from part 1*/
SELECT
  `Medical Condition`,
  Hospital,
  ROUND(AVG(`Billing Amount`), 2)                 AS avg_total_bill,
  ROUND(AVG(
    SAFE_DIVIDE(
      `Billing Amount`,
      DATE_DIFF(`Discharge Date`, `Date of Admission`, DAY)
    )
  ), 2)                                           AS avg_cost_per_day,
  COUNTIF(
    DATE_DIFF(`Discharge Date`, `Date of Admission`, DAY) = 0
  )                                               AS zero_stay_anomalies  -- flags data issues
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
GROUP BY `Medical Condition`, Hospital
ORDER BY avg_cost_per_day DESC;

/* Part 3 - with info from part 1 */
SELECT
  `Medical Condition`,
  `Admission Type`,
  COUNT(*) AS frequency
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
WHERE Hospital = 'Smith'
GROUP BY `Medical Condition`, `Admission Type`
ORDER BY `Admission Type`, frequency DESC;


/* Test Results for each Admission Type */
SELECT
  `Admission Type`,
  `Test Results`,
  COUNT(*) AS total,
  ROUND(
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(),
    2
  ) AS percentage_of_total
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
GROUP BY `Admission Type`, `Test Results`
ORDER BY `Test Results`, total DESC;

/* Which admission type is most expensive?*/
SELECT
`Admission Type`,
CASE
WHEN `Billing Amount` > 40000 THEN 'High Cost'
WHEN `Billing Amount` BETWEEN 20000 AND 40000 THEN 'Medium Cost'
ELSE 'Low Cost'
END AS cost_category,
COUNT(*) AS cases
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
GROUP BY `Admission Type`, cost_category
ORDER BY `Admission Type`;


/* What proportion of admissions require further medical attention?*/

SELECT
CASE
WHEN `Test Results` = 'Abnormal' THEN 'Requires Attention'
WHEN `Test Results` = 'Normal' THEN 'Stable'
ELSE 'Inconclusive'
END AS result_category,
COUNT(*) AS total_cases
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
GROUP BY result_category;

/* How do admission counts compare between male and female patients?*/
SELECT
'Male' AS gender_group,
COUNT(*) AS admissions
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
WHERE Gender = 'Male'

UNION ALL

SELECT
'Female',
COUNT(*)
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
WHERE Gender = 'Female';


/* Are emergency admissions more common than elective admissions?*/

SELECT
'Emergency Admissions' AS category,
COUNT(*) AS total
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
WHERE `Admission Type` = 'Emergency'

UNION ALL

SELECT
'Elective Admissions',
COUNT(*)
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
WHERE `Admission Type` = 'Elective';


/* How does the average billing amount compare with average daily treatment cost?*/
SELECT
'Average Billing' AS metric,
AVG(`Billing Amount`) AS value
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`

UNION ALL

SELECT
'Average Cost Per Day',
AVG(`Billing Amount` / NULLIF(DATE_DIFF(`Discharge Date`,`Date of Admission`,DAY),0))
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`;
