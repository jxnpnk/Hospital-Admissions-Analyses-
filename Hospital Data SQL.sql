SELECT *
FROM `healthcare-admissions-dataset.hospital_analysis.admissions`
LIMIT 100;


SELECT
COUNT(*) AS total_rows,
COUNTIF(Name IS NULL) AS missing_name,
COUNTIF(`Date of Admission` IS NULL) AS missing_admission_date
FROM `healthcare-admissions-dataset.hospital_analysis.admissions`;


CREATE OR REPLACE TABLE `healthcare-admissions-dataset.hospital_analysis.admissions_clean` AS
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
LIMIT 100;


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

SELECT
EXTRACT(YEAR FROM `Date of Admission`)AS year,
EXTRACT(MONTH FROM `Date of Admission`)AS month,
COUNT(*)AS admissions
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
GROUP BY year,month
ORDER BY year,month;


/*Gender Analyses */
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
WHERE condition_rank <= 3;


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
ORDER BY Hospital;

/* Part 2 - with info from part 1*/
SELECT
  `Medical Condition`,
  AVG(DATE_DIFF(`Discharge Date`, `Date of Admission`, DAY)) AS avg_stay,
  AVG(`Billing Amount`) AS avg_bill,
  AVG(`Billing Amount` / NULLIF(DATE_DIFF(`Discharge Date`, `Date of Admission`, DAY), 0)) AS avg_cost_per_day
FROM `healthcare-admissions-dataset.hospital_analysis.admissions_clean`
WHERE Hospital = 'Smith'
GROUP BY `Medical Condition`
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
