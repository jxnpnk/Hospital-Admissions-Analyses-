--========================================================================================================================
--DEBUGGING SCENARIO 1: Conversion Errors

/* Context: A common real-world pipeline failure. Data arrives from an external source with inconsistent formatting — values 
that should be numeric contain symbols, spaces, or text. Without defensive handling the entire process crashes.

This scenario simulates exactly what happened during the initial BULK INSERT of this dataset — Billing_Amount and date columns 
failed to load due to type mismatches. 

Simulates raw data arriving into a staging table as NVARCHAR before being loaded into the main table. */

--STEP 1: Create staging table with NVARCHAR columns
CREATE TABLE admissions_staging (
    Name                NVARCHAR(100),
    Medical_Condition   NVARCHAR(100),
    Billing_Amount      NVARCHAR(50),    --intentionally NVARCHAR
    Admission_Date      NVARCHAR(50),    --intentionally NVARCHAR
    Discharge_Date      NVARCHAR(50)     --intentionally NVARCHAR
);
GO

--STEP 2: Insert mix of valid and invalid rows
INSERT INTO admissions_staging VALUES
('John Smith',   'Diabetes',  '25000.50',    '2024-01-01', '2024-01-05'),
('Jane Doe',     'Cancer',    '$NotANumber', '2024-02-01', '2024-02-10'),
('Bob Johnson',  'Obesity',   '31500.75',    '2024-03-01', '2024-03-08'),
('Test Patient', 'Arthritis', 'N/A',         '2024-04-01', '2024-04-03');
GO

--STEP 3: Reproduce the problem
--Naive direct CAST — crashes on first bad row.
--Expected error: Msg 8114

SELECT
    Name,
    CAST(Billing_Amount AS FLOAT) AS billing_converted
FROM admissions_staging;
GO

--STEP 4: Diagnose using TRY_CAST
--Identifies exactly which rows will fail before attempting the load — this is the investigation step.

SELECT
    Name,
    Billing_Amount                          AS raw_value,
    TRY_CAST(Billing_Amount AS FLOAT)       AS attempted_conversion,
    CASE
        WHEN TRY_CAST(Billing_Amount AS FLOAT) IS NULL
        THEN 'CONVERSION FAILED — investigate source row'
        ELSE 'Valid'
    END                                     AS diagnosis
FROM admissions_staging;
GO

--STEP 5: The fix — TRY_CATCH with error logging
--Loops through staging rows, attempts conversion on each.
--Valid rows get inserted into main table.
--Failed rows get logged to error_log for investigation.

DECLARE @Name               NVARCHAR(100);
DECLARE @Medical_Condition  NVARCHAR(100);
DECLARE @Billing_Amount     NVARCHAR(50);

DECLARE staging_cursor CURSOR FOR
    SELECT Name, Medical_Condition, Billing_Amount
    FROM admissions_staging;

OPEN staging_cursor;
FETCH NEXT FROM staging_cursor INTO @Name, @Medical_Condition, @Billing_Amount;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        INSERT INTO admissions_1 (Name, Medical_Condition, Billing_Amount)
        VALUES (@Name, @Medical_Condition, CAST(@Billing_Amount AS DECIMAL(18,2)));

        PRINT 'Row inserted successfully: ' + @Name;
    END TRY
    BEGIN CATCH
        INSERT INTO error_log (
            error_number,
            error_severity,
            error_state,
            error_procedure,
            error_line,
            error_message,
            process_context
        )
        VALUES (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE(),
            'Scenario 1 — Billing_Amount conversion failure: ' + @Name
        );

        PRINT 'Conversion failed for: ' + @Name + ' — logged to error_log';
    END CATCH;

    FETCH NEXT FROM staging_cursor INTO @Name, @Medical_Condition, @Billing_Amount;
END;

CLOSE staging_cursor;
DEALLOCATE staging_cursor;
GO

--STEP 6: Verify error log captured the bad rows
SELECT
    error_id,
    error_message,
    error_datetime,
    process_context
FROM error_log;
GO

--STEP 7: Clean up
DELETE FROM admissions_1 
WHERE Name IN ('John Smith', 'Jane Doe', 'Bob Johnson', 'Test Patient');

DROP TABLE admissions_staging;
GO


--========================================================================================================================
--DEBUGGING SCENARIO 2: Logging Errors to a Table

/*--Context: In production, SQL Agent jobs run stored procedures on a schedule. When they fail, errors need to be logged automatically
 with enough context to investigate later — procedure name, line number, and what process was running. */


--STEP 1: Create a stored procedure that will fail
--Simulates a nightly billing summary process. Contains a deliberate divide-by-zero to trigger a catchable error.

CREATE OR ALTER PROCEDURE usp_generate_billing_summary
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        --Simulate billing summary calculation
        SELECT
            Medical_Condition,
            AVG(Billing_Amount)                             AS avg_billing,
            --Deliberate error: dividing by zero to simulate
            --a process failure mid-execution
            SUM(Billing_Amount) / 0                         AS faulty_calculation
        FROM admissions_1
        GROUP BY Medical_Condition;

    END TRY
    BEGIN CATCH
        --Log full error context including procedure and line
        INSERT INTO error_log (
            error_number,
            error_severity,
            error_state,
            error_procedure,
            error_line,
            error_message,
            process_context
        )
        VALUES (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),      --captures 'usp_generate_billing_summary'
            ERROR_LINE(),           --captures exact line number of failure
            ERROR_MESSAGE(),
            'Nightly billing summary job — usp_generate_billing_summary'
        );

        --Re-raise a clean error to the calling process
        THROW 50001, 'Billing summary process failed — error logged for investigation.', 1;
    END CATCH;
END;
GO


--STEP 2: Execute the procedure to trigger the failure

EXEC usp_generate_billing_summary;
GO


--STEP 3: Investigate the error log
--This is what a support engineer would run first when a job fails — checking the log to identify what failed, where, and when.

SELECT
    error_id,
    error_number,
    error_severity,
    error_procedure,
    error_line,
    error_message,
    error_datetime,
    process_context
FROM error_log
ORDER BY error_datetime DESC;
GO


--STEP 4: Fix the stored procedure
--Replace the faulty calculation with NULLIF protection.
--Document the fix in a comment as you would in a real code change.

CREATE OR ALTER PROCEDURE usp_generate_billing_summary
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        SELECT
            Medical_Condition,
            AVG(Billing_Amount)                             AS avg_billing,
            --FIX: NULLIF prevents divide-by-zero
            --Original issue logged in error_log, error_id 1
            SUM(Billing_Amount) /
            NULLIF(COUNT(*), 0)                             AS avg_billing_per_case
        FROM admissions_1
        GROUP BY Medical_Condition;

    END TRY
    BEGIN CATCH
        INSERT INTO error_log (
            error_number,
            error_severity,
            error_state,
            error_procedure,
            error_line,
            error_message,
            process_context
        )
        VALUES (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE(),
            'Nightly billing summary job — usp_generate_billing_summary'
        );

        THROW 50001, 'Billing summary process failed — error logged for investigation.', 1;
    END CATCH;
END;
GO


--STEP 5: Re-execute to confirm fix works

EXEC usp_generate_billing_summary;
GO


--STEP 6: Check error log shows original failure preserved
--Important: the original error entry is kept in the log.
--In production you never delete error history — it forms the audit trail for the incident.

SELECT
    error_id,
    error_number,
    error_procedure,
    error_line,
    error_message,
    error_datetime,
    process_context
FROM error_log
ORDER BY error_datetime DESC;
GO

--========================================================================================================================
--DEBUGGING SCENARIO 3: Divide-by-Zero Errors

/*Context: Financial calculations in regulatory reporting frequently involve division — cost per day, average billing per case, ratios.  
A zero denominator crashes the process.

This scenario demonstrates three levels of protection:
1. Reproducing the error
2. NULLIF as inline protection
3. TRY_CATCH for full pipeline protection with logging
*/

--STEP 1: Create a scenario where divide-by-zero can occur
--Simulates a same-day discharge (admission = discharge date) which produces a zero length of stay — a real data quality issue 
--in healthcare datasets.


CREATE TABLE billing_test (
    Patient_Name        NVARCHAR(100),
    Billing_Amount      DECIMAL(18,2),
    Admission_Date      DATE,
    Discharge_Date      DATE
);
GO

INSERT INTO billing_test VALUES
('Alice Brown',  25000.00, '2024-01-01', '2024-01-05'),  --normal stay
('Bob Clarke',   18000.00, '2024-02-01', '2024-02-01'),  --same day — zero stay
('Carol Davies', 31000.00, '2024-03-01', '2024-03-08'),  --normal stay
('David Evans',  22000.00, '2024-04-01', '2024-04-01');  --same day — zero stay
GO


--STEP 2: Reproduce the error
--Direct division with no protection.

SELECT
    Patient_Name,
    Billing_Amount,
    DATEDIFF(DAY, Admission_Date, Discharge_Date)       AS stay_days,
    Billing_Amount /
    DATEDIFF(DAY, Admission_Date, Discharge_Date)       AS cost_per_day
FROM billing_test;
GO


--STEP 3: Diagnose — identify the problem rows first
--Before fixing, identify which rows will cause the failure.
--This is the investigation step.

SELECT
    Patient_Name,
    Billing_Amount,
    DATEDIFF(DAY, Admission_Date, Discharge_Date)       AS stay_days,
    CASE
        WHEN DATEDIFF(DAY, Admission_Date, Discharge_Date) = 0
        THEN 'ZERO STAY — will cause divide-by-zero'
        ELSE 'Valid'
    END                                                 AS diagnosis
FROM billing_test;
GO


--STEP 4: Fix using NULLIF
--NULLIF returns NULL when stay_days = 0, preventing the division from executing on those rows.
--NULL cost_per_day flags the row for further investigation rather than crashing the process.

SELECT
    Patient_Name,
    Billing_Amount,
    DATEDIFF(DAY, Admission_Date, Discharge_Date)       AS stay_days,
    ROUND(
        Billing_Amount /
        NULLIF(DATEDIFF(DAY, Admission_Date, Discharge_Date), 0)
    , 2)                                                AS cost_per_day,
    CASE
        WHEN DATEDIFF(DAY, Admission_Date, Discharge_Date) = 0
        THEN 'Flagged — same day discharge, review source data'
        ELSE 'Valid'
    END                                                 AS data_quality_flag
FROM billing_test;
GO


--STEP 5: Full pipeline protection with TRY_CATCH
--Wraps the calculation in a stored procedure with full error handling — if anything fails, it logs and continues.

CREATE OR ALTER PROCEDURE usp_calculate_cost_per_day
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        SELECT
            Patient_Name,
            Billing_Amount,
            DATEDIFF(DAY, Admission_Date, Discharge_Date)   AS stay_days,
            ROUND(
                Billing_Amount /
                NULLIF(DATEDIFF(
                    DAY, Admission_Date, Discharge_Date
                ), 0)
            , 2)                                            AS cost_per_day,
            CASE
                WHEN DATEDIFF(
                    DAY, Admission_Date, Discharge_Date
                ) = 0
                THEN 'Flagged — same day discharge'
                ELSE 'Valid'
            END                                             AS data_quality_flag
        FROM billing_test;

    END TRY
    BEGIN CATCH
        INSERT INTO error_log (
            error_number,
            error_severity,
            error_state,
            error_procedure,
            error_line,
            error_message,
            process_context
        )
        VALUES (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE(),
            'Scenario 3 — cost per day calculation: usp_calculate_cost_per_day'
        );

        THROW 50002, 'Cost per day calculation failed — error logged.', 1;
    END CATCH;
END;
GO


--STEP 6: Execute and confirm fix works
EXEC usp_calculate_cost_per_day;
GO


--STEP 7: Clean up
DROP TABLE billing_test;
GO

--=============================================================================================================================
--DEBUGGING SCENARIO 4: Missing Index Recommendations
/*Context: A regulatory reporting query is running slowly.
The investigation involves identifying whether missing indexes are the root cause, evaluating the recommendation, implementing 
the fix, and measuring the improvement.*/


--STEP 1: Create a larger test table to make index impact visible
--Populates a test table with enough rows that a table scan is noticeably slower than an index seek.

CREATE TABLE admissions_perf_test (
    admission_id        INT IDENTITY(1,1) PRIMARY KEY,
    Patient_Name        NVARCHAR(100),
    Medical_Condition   NVARCHAR(100),
    Hospital            NVARCHAR(100),
    Admission_Type      NVARCHAR(50),
    Billing_Amount      DECIMAL(18,2),
    Admission_Date      DATE,
    Discharge_Date      DATE,
    Test_Results        NVARCHAR(50)
);
GO

--Populate with data from your main table
INSERT INTO admissions_perf_test (
    Patient_Name, Medical_Condition, Hospital,
    Admission_Type, Billing_Amount, Admission_Date,
    Discharge_Date, Test_Results
)
SELECT
    Name, Medical_Condition, Hospital_canonical,
    Admission_Type, Billing_Amount, Admission_Date_clean,
    Discharge_Date_clean, Test_Results
FROM admissions_1;
GO

--Check row count loaded
SELECT COUNT(*) AS rows_loaded FROM admissions_perf_test;
GO


--STEP 2: Run the slow query — no indexes exist yet
/*This query simulates a regulatory report filtering on Admission_Type and Medical_Condition. Without indexes SQL Server 
performs a full table scan.
Enable STATISTICS IO to measure logical reads before fix.
Logical reads = number of data pages read — lower is better. */

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

SELECT
    Hospital,
    Medical_Condition,
    Admission_Type,
    COUNT(*)                        AS total_cases,
    AVG(Billing_Amount)             AS avg_billing,
    AVG(DATEDIFF(
        DAY, Admission_Date, Discharge_Date
    ))                              AS avg_stay_days
FROM admissions_perf_test
WHERE Admission_Type = 'Emergency'
AND Medical_Condition = 'Cancer'
GROUP BY Hospital, Medical_Condition, Admission_Type
ORDER BY total_cases DESC;

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


--STEP 3: Check if SQL Server is recommending a missing index
--SQL Server tracks queries that would benefit from indexes in sys.dm_db_missing_index_details. This is what a support engineer checks 
--when investigating a slow query.

SELECT
    mid.statement                           AS table_name,
    migs.avg_total_user_cost                AS avg_query_cost,
    migs.avg_user_impact                    AS estimated_improvement_pct,
    migs.user_seeks                         AS times_seek_needed,
    mid.equality_columns,
    mid.inequality_columns,
    mid.included_columns
FROM sys.dm_db_missing_index_groups         mig
JOIN sys.dm_db_missing_index_group_stats    migs
    ON mig.index_group_handle = migs.group_handle
JOIN sys.dm_db_missing_index_details        mid
    ON mig.index_handle = mid.index_handle
WHERE mid.statement LIKE '%admissions_perf_test%'
ORDER BY migs.avg_user_impact DESC;
GO


--STEP 4: Implement the recommended index
/*Creates a composite non-clustered index on the filter columns, with billing and stay calculation columns included to
 make it a covering index — meaning SQL Server can satisfy the entire query from the index alone without touching the main table.*/

CREATE NONCLUSTERED INDEX IX_admissions_perf_test_type_condition
ON admissions_perf_test (Admission_Type, Medical_Condition)
INCLUDE (Hospital, Billing_Amount, Admission_Date, Discharge_Date);
GO


--STEP 5: Re-run the same query and compare
--Run the identical query again with STATISTICS IO.
--Compare logical reads before and after — the reduction
--confirms the index is being used and the fix worked.

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

SELECT
    Hospital,
    Medical_Condition,
    Admission_Type,
    COUNT(*)                        AS total_cases,
    AVG(Billing_Amount)             AS avg_billing,
    AVG(DATEDIFF(
        DAY, Admission_Date, Discharge_Date
    ))                              AS avg_stay_days
FROM admissions_perf_test
WHERE Admission_Type = 'Emergency'
AND Medical_Condition = 'Cancer'
GROUP BY Hospital, Medical_Condition, Admission_Type
ORDER BY total_cases DESC;

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


--STEP 6: Confirm the index is being used
--sys.dm_exec_query_stats shows recent query execution stats.
--Check that logical reads dropped after the index was added.

SELECT
    qs.execution_count,
    qs.total_logical_reads,
    qs.total_worker_time                    AS cpu_time_microseconds,
    SUBSTRING(
        qt.text, 1, 200
    )                                       AS query_text
FROM sys.dm_exec_query_stats               qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
WHERE qt.text LIKE '%admissions_perf_test%'
AND qt.text NOT LIKE '%dm_exec%'
ORDER BY qs.total_logical_reads DESC;
GO


--STEP 7: Check index fragmentation
--After adding an index, check its fragmentation level.
--Above 30% fragmentation = rebuild. Between 10-30% = reorganise.

SELECT
    i.name                                  AS index_name,
    ips.avg_fragmentation_in_percent,
    ips.page_count,
    CASE
        WHEN ips.avg_fragmentation_in_percent > 30
        THEN 'REBUILD recommended'
        WHEN ips.avg_fragmentation_in_percent BETWEEN 10 AND 30
        THEN 'REORGANISE recommended'
        ELSE 'OK — no action needed'
    END                                     AS recommended_action
FROM sys.dm_db_index_physical_stats(
    DB_ID(), OBJECT_ID('admissions_perf_test'), NULL, NULL, 'LIMITED'
) ips
JOIN sys.indexes i
    ON ips.object_id = i.object_id
    AND ips.index_id = i.index_id
WHERE i.name IS NOT NULL;
GO


--STEP 8: Clean up
DROP TABLE admissions_perf_test;
GO

--========================================================================================================================
--DEBUGGING SCENARIO 5: Constraint Violations

/*Context:If the source data contains duplicates or values that violate business rules, the entire insert fails without proper handling. 
This scenario simulates a bulk load of  admissions data that contains duplicate patient records and an out-of-range billing value
both common real-world data quality issues.
*/

--STEP 1: Create a table with constraints
--Unique constraint on Name + Admission_Date prevents duplicate admissions for the same patient on the same day.
--Check constraint enforces a valid billing range.

CREATE TABLE admissions_constrained (
    admission_id        INT IDENTITY(1,1) PRIMARY KEY,
    Patient_Name        NVARCHAR(100)   NOT NULL,
    Medical_Condition   NVARCHAR(100),
    Billing_Amount      DECIMAL(18,2),
    Admission_Date      DATE,
    Discharge_Date      DATE,
    CONSTRAINT UQ_patient_admission
        UNIQUE (Patient_Name, Admission_Date),
    CONSTRAINT CHK_billing_range
        CHECK (Billing_Amount BETWEEN 0 AND 150000)
);
GO


--STEP 2: Insert clean baseline data

INSERT INTO admissions_constrained
    (Patient_Name, Medical_Condition, Billing_Amount, Admission_Date, Discharge_Date)
VALUES
('Alice Brown',  'Cancer',   25000.00, '2024-01-01', '2024-01-08'),
('Bob Clarke',   'Diabetes', 18000.00, '2024-01-02', '2024-01-06'),
('Carol Davies', 'Obesity',  31000.00, '2024-01-03', '2024-01-10');
GO


--STEP 3: Reproduce the problem
/*
Simulate an incoming batch containing:
Row 1 — valid new record
Row 2 — duplicate (Alice Brown same date) violates UQ_patient_admission
Row 3 — billing amount exceeds 150000 violates CHK_billing_range
Row 4 — valid new record

Without constraint handling the entire batch fails and nothing is inserted — including the valid rows.
*/

INSERT INTO admissions_constrained
    (Patient_Name, Medical_Condition, Billing_Amount, Admission_Date, Discharge_Date)
VALUES
('David Evans',  'Arthritis', 22000.00, '2024-01-04', '2024-01-07'),  --valid
('Alice Brown',  'Cancer',    25000.00, '2024-01-01', '2024-01-08'),  --duplicate
('Frank Harris', 'Asthma',   999999.00, '2024-01-05', '2024-01-06'),  --bad billing
('Grace Jones',  'Diabetes',  17000.00, '2024-01-06', '2024-01-09');  --valid
GO


--STEP 4: Diagnose — check for duplicates before inserting
--This is the pre-insert validation step. Identifies which rows from the incoming batch would violate constraints before attempting the load.

--Check for duplicates against existing data
SELECT
    s.Patient_Name,
    s.Admission_Date,
    'DUPLICATE — violates UQ_patient_admission'     AS violation_type
FROM (VALUES
    ('David Evans',  CAST('2024-01-04' AS DATE)),
    ('Alice Brown',  CAST('2024-01-01' AS DATE)),
    ('Frank Harris', CAST('2024-01-05' AS DATE)),
    ('Grace Jones',  CAST('2024-01-06' AS DATE))
) AS s(Patient_Name, Admission_Date)
JOIN admissions_constrained ac
    ON s.Patient_Name = ac.Patient_Name
    AND s.Admission_Date = ac.Admission_Date

UNION ALL

--Check for billing range violations
SELECT
    Patient_Name,
    Admission_Date,
    'BILLING OUT OF RANGE — violates CHK_billing_range'
FROM (VALUES
    ('David Evans',  CAST('2024-01-04' AS DATE), 22000.00),
    ('Alice Brown',  CAST('2024-01-01' AS DATE), 25000.00),
    ('Frank Harris', CAST('2024-01-05' AS DATE), 999999.00),
    ('Grace Jones',  CAST('2024-01-06' AS DATE), 17000.00)
) AS s(Patient_Name, Admission_Date, Billing_Amount)
WHERE Billing_Amount NOT BETWEEN 0 AND 150000;
GO


--STEP 5: The fix — row by row insert with TRY_CATCH
--Processes each row individually. Valid rows are inserted.
--Constraint violations are caught, logged, and skipped without failing the entire batch.

DECLARE @Patient_Name       NVARCHAR(100);
DECLARE @Medical_Condition  NVARCHAR(100);
DECLARE @Billing_Amount     DECIMAL(18,2);
DECLARE @Admission_Date     DATE;
DECLARE @Discharge_Date     DATE;

DECLARE batch_cursor CURSOR FOR
    SELECT Patient_Name, Medical_Condition, Billing_Amount,
           Admission_Date, Discharge_Date
    FROM (VALUES
        ('David Evans',  'Arthritis', CAST(22000.00  AS DECIMAL(18,2)), CAST('2024-01-04' AS DATE), CAST('2024-01-07' AS DATE)),
        ('Alice Brown',  'Cancer',    CAST(25000.00  AS DECIMAL(18,2)), CAST('2024-01-01' AS DATE), CAST('2024-01-08' AS DATE)),
        ('Frank Harris', 'Asthma',    CAST(999999.00 AS DECIMAL(18,2)), CAST('2024-01-05' AS DATE), CAST('2024-01-06' AS DATE)),
        ('Grace Jones',  'Diabetes',  CAST(17000.00  AS DECIMAL(18,2)), CAST('2024-01-06' AS DATE), CAST('2024-01-09' AS DATE))
    ) AS batch(Patient_Name, Medical_Condition, Billing_Amount,
               Admission_Date, Discharge_Date);

OPEN batch_cursor;
FETCH NEXT FROM batch_cursor INTO
    @Patient_Name, @Medical_Condition, @Billing_Amount,
    @Admission_Date, @Discharge_Date;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        INSERT INTO admissions_constrained
            (Patient_Name, Medical_Condition, Billing_Amount,
             Admission_Date, Discharge_Date)
        VALUES
            (@Patient_Name, @Medical_Condition, @Billing_Amount,
             @Admission_Date, @Discharge_Date);

        PRINT 'Inserted successfully: ' + @Patient_Name;
    END TRY
    BEGIN CATCH
        INSERT INTO error_log (
            error_number,
            error_severity,
            error_state,
            error_procedure,
            error_line,
            error_message,
            process_context
        )
        VALUES (
            ERROR_NUMBER(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            ERROR_PROCEDURE(),
            ERROR_LINE(),
            ERROR_MESSAGE(),
            'Scenario 5 — constraint violation: ' + @Patient_Name
        );

        PRINT 'Constraint violation for: ' + @Patient_Name
            + ' — logged to error_log';
    END CATCH;

    FETCH NEXT FROM batch_cursor INTO
        @Patient_Name, @Medical_Condition, @Billing_Amount,
        @Admission_Date, @Discharge_Date;
END;

CLOSE batch_cursor;
DEALLOCATE batch_cursor;
GO


--STEP 6: Verify valid rows were inserted, bad rows were skipped

SELECT * FROM admissions_constrained ORDER BY admission_id;
GO


--STEP 7: Verify constraint violations were logged

SELECT
    error_id,
    error_number,
    error_message,
    error_datetime,
    process_context
FROM error_log
WHERE process_context LIKE '%Scenario 5%'
ORDER BY error_datetime DESC;
GO


--STEP 8: Clean up
DROP TABLE admissions_constrained;
GO

--========================================================================================================================
--DEBUGGING SCENARIO 6: Implicit Conversion Issues
/* Context: Implicit conversions occur when SQL Server silently converts one data type to another to complete a comparison. 
This prevents index usage and causes full table scans — a hidden performance issue that only 
becomes visible when reading execution plans.
*/

--STEP 1: Create a table with typed columns and an index
--Simulates a transactions table where Account_Number is stored as NVARCHAR — common in financial/regulatory systems where account 
--numbers may contain leading zeros or letters.

CREATE TABLE regulatory_transactions (
    transaction_id      INT IDENTITY(1,1) PRIMARY KEY,
    Account_Number      NVARCHAR(20)    NOT NULL,
    Transaction_Date    DATE            NOT NULL,
    Amount              DECIMAL(18,2),
    Transaction_Type    NVARCHAR(50),
    Status              NVARCHAR(20)
);
GO

--Create an index on Account_Number — this is what we expect
--the query to use. Implicit conversion will bypass it.
CREATE NONCLUSTERED INDEX IX_regulatory_account
ON regulatory_transactions (Account_Number);
GO

--Populate with test data
INSERT INTO regulatory_transactions
    (Account_Number, Transaction_Date, Amount, Transaction_Type, Status)
SELECT TOP 10000
    CAST(ABS(CHECKSUM(NEWID())) % 900000 + 100000 AS NVARCHAR(20)),
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 365, GETDATE()),
    CAST(ABS(CHECKSUM(NEWID())) % 50000 + 1000 AS DECIMAL(18,2)),
    CASE ABS(CHECKSUM(NEWID())) % 3
        WHEN 0 THEN 'Deposit'
        WHEN 1 THEN 'Withdrawal'
        ELSE 'Transfer'
    END,
    CASE ABS(CHECKSUM(NEWID())) % 2
        WHEN 0 THEN 'Cleared'
        ELSE 'Pending'
    END
FROM sys.objects a
CROSS JOIN sys.objects b;
GO


--STEP 2: Reproduce the implicit conversion problem
/* Passing an INT parameter to a column defined as NVARCHAR. SQL Server silently converts every value in Account_Number to INT for 
the comparison — index cannot be used. */

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

DECLARE @account_search INT = 123456;  --INT, not NVARCHAR

SELECT
    transaction_id,
    Account_Number,
    Amount,
    Transaction_Type,
    Status
FROM regulatory_transactions
WHERE Account_Number = @account_search;  --implicit conversion here

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


--STEP 3: Diagnose — identify implicit conversion in plan cache
--Queries sys.dm_exec_query_stats to find queries with implicit conversion warnings in their execution plans.

SELECT
    qs.execution_count,
    qs.total_logical_reads,
    qs.total_worker_time                        AS cpu_microseconds,
    SUBSTRING(qt.text, 1, 300)                  AS query_text,
    qp.query_plan
FROM sys.dm_exec_query_stats                    qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
WHERE qt.text LIKE '%regulatory_transactions%'
AND qt.text NOT LIKE '%dm_exec%'
ORDER BY qs.total_logical_reads DESC;
GO


--STEP 4: Diagnose — check column data types
--Confirms the mismatch between column definition and  the parameter type being passed — the root cause.

SELECT
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'regulatory_transactions'
AND COLUMN_NAME = 'Account_Number';
GO


--STEP 5: The fix — explicit type matching
--Change the parameter type to match the column definition. SQL Server can now use the index as intended. 
--Compare logical reads with Step 2.

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

DECLARE @account_search_fixed NVARCHAR(20) = '123456';  --NVARCHAR matches column

SELECT
    transaction_id,
    Account_Number,
    Amount,
    Transaction_Type,
    Status
FROM regulatory_transactions
WHERE Account_Number = @account_search_fixed;  --no implicit conversion

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


--STEP 6: Alternative fix — CAST in query
/* If you cannot change the parameter type (e.g. it comes from an application layer you do not control), explicitly cast the 
parameter instead. Less ideal than Step 5 but documents the intent clearly. */

DECLARE @account_search_app INT = 123456;

SELECT
    transaction_id,
    Account_Number,
    Amount,
    Transaction_Type,
    Status
FROM regulatory_transactions
WHERE Account_Number = CAST(@account_search_app AS NVARCHAR(20));
GO


--STEP 7: Document the finding in the error log
--Implicit conversions don't throw errors — they're silent performance issues. Logging the finding manually creates an audit trail for the investigation.

INSERT INTO error_log (
    error_number,
    error_severity,
    error_state,
    error_procedure,
    error_line,
    error_message,
    process_context
)
VALUES (
    0,
    0,
    0,
    NULL,
    NULL,
    'Implicit conversion detected on regulatory_transactions.Account_Number — '
    + 'INT parameter passed to NVARCHAR column. Index IX_regulatory_account '
    + 'bypassed causing table scan. Fixed by aligning parameter type to NVARCHAR(20).',
    'Scenario 6 — implicit conversion investigation'
);
GO


--STEP 8: Verify finding is logged
SELECT
    error_id,
    error_message,
    error_datetime,
    process_context
FROM error_log
WHERE process_context LIKE '%Scenario 6%';
GO


--STEP 9: Clean up
DROP TABLE regulatory_transactions;
GO


--========================================================================================================================
--DEBUGGING SCENARIO 7: Recursive CTE Debugging
/*Context: Recursive CTEs are used in regulatory and healthcare systems to traverse hierarchical data — organisational structures, 
referral chains, case escalation paths. A poorly written recursive CTE can either hit SQL Server's recursion limit and crash, or
loop infinitely and never return.
This scenario builds a hospital referral chain; patients referred between departments and demonstrates how to debug termination issues.
*/

--STEP 1: Create a referral chain table
--Models a patient referral network where patients are referred from one department to another for treatment.

CREATE TABLE patient_referrals (
    referral_id         INT PRIMARY KEY,
    patient_name        NVARCHAR(100),
    from_department     NVARCHAR(100),
    to_department       NVARCHAR(100),
    parent_referral_id  INT NULL        --NULL = initial admission
);
GO

INSERT INTO patient_referrals VALUES
(1,  'Alice Brown',  NULL,          'A&E',           NULL),
(2,  'Alice Brown',  'A&E',         'Cardiology',    1),
(3,  'Alice Brown',  'Cardiology',  'ICU',           2),
(4,  'Alice Brown',  'ICU',         'General Ward',  3),
(5,  'Bob Clarke',   NULL,          'A&E',           NULL),
(6,  'Bob Clarke',   'A&E',         'Orthopaedics',  5),
(7,  'Bob Clarke',   'Orthopaedics','General Ward',  6);
GO


--STEP 2: Reproduce the problem — missing termination condition
--A recursive CTE with an incorrect WHERE clause that never reaches a base case. SQL Server hits the default recursion limit of 100 and throws an error.

WITH referral_chain AS (
    --Anchor: start from all referrals (missing NULL filter)
    SELECT
        referral_id,
        patient_name,
        from_department,
        to_department,
        parent_referral_id,
        1 AS depth
    FROM patient_referrals

    UNION ALL

    --Recursive member: missing correct join condition joins back to itself without a valid termination
    SELECT
        pr.referral_id,
        pr.patient_name,
        pr.from_department,
        pr.to_department,
        pr.parent_referral_id,
        rc.depth + 1
    FROM patient_referrals pr
    INNER JOIN referral_chain rc
        ON pr.parent_referral_id = rc.parent_referral_id  --wrong join
    WHERE rc.depth < 100
)
SELECT * FROM referral_chain
OPTION (MAXRECURSION 10);  --limit set low to fail fast during debugging
GO


--STEP 3: Diagnose — validate the anchor and termination
--Before fixing the recursive member, verify the anchor returns the correct starting rows — initial admissions where parent_referral_id IS NULL.

SELECT
    referral_id,
    patient_name,
    from_department,
    to_department,
    parent_referral_id
FROM patient_referrals
WHERE parent_referral_id IS NULL;  --should return 2 rows
GO


--STEP 4: Diagnose — trace the recursion manually
--Walk through what the recursive member should do at each level. This confirms the join condition before fixing.

SELECT
    child.referral_id,
    child.patient_name,
    child.from_department,
    child.to_department,
    child.parent_referral_id,
    parent.referral_id          AS parent_id
FROM patient_referrals          child
JOIN patient_referrals          parent
    ON child.parent_referral_id = parent.referral_id  --correct join
WHERE child.parent_referral_id IS NOT NULL;
GO


--STEP 5: The fix — correct anchor and join condition
--Anchor starts from initial admissions only (NULL parent).
--Recursive member joins child.parent_referral_id to parent.referral_id — this guarantees termination because the chain has a finite depth.

WITH referral_chain_fixed AS (
    --Anchor: initial admissions only
    SELECT
        referral_id,
        patient_name,
        from_department,
        to_department,
        parent_referral_id,
        1                           AS depth,
        CAST(patient_name + ' > '
            + to_department
        AS NVARCHAR(1000))          AS referral_path
    FROM patient_referrals
    WHERE parent_referral_id IS NULL

    UNION ALL

    --Recursive member: correct join condition
    SELECT
        pr.referral_id,
        pr.patient_name,
        pr.from_department,
        pr.to_department,
        pr.parent_referral_id,
        rc.depth + 1,
        CAST(rc.referral_path + ' > '
            + pr.to_department
        AS NVARCHAR(1000))
    FROM patient_referrals          pr
    INNER JOIN referral_chain_fixed rc
        ON pr.parent_referral_id = rc.referral_id  --correct join
)
SELECT
    patient_name,
    depth,
    from_department,
    to_department,
    referral_path
FROM referral_chain_fixed
ORDER BY patient_name, depth
OPTION (MAXRECURSION 50);  --explicit limit as safety net
GO


--STEP 6: Add MAXRECURSION guard for production safety
--Even with a correct termination condition, always set an explicit MAXRECURSION limit in production. This prevents unexpected 
--data cycles from causing infinite loops. 
--0 = unlimited (dangerous), default = 100.

WITH referral_chain_safe AS (
    SELECT
        referral_id,
        patient_name,
        to_department,
        parent_referral_id,
        1                           AS depth
    FROM patient_referrals
    WHERE parent_referral_id IS NULL

    UNION ALL

    SELECT
        pr.referral_id,
        pr.patient_name,
        pr.to_department,
        pr.parent_referral_id,
        rc.depth + 1
    FROM patient_referrals          pr
    INNER JOIN referral_chain_safe  rc
        ON pr.parent_referral_id = rc.referral_id
    WHERE rc.depth < 10             --inline depth guard as extra protection
)
SELECT
    patient_name,
    depth,
    to_department
FROM referral_chain_safe
ORDER BY patient_name, depth
OPTION (MAXRECURSION 10);
GO


--STEP 7: Log the investigation finding

INSERT INTO error_log (
    error_number,
    error_severity,
    error_state,
    error_procedure,
    error_line,
    error_message,
    process_context
)
VALUES (
    530,
    16,
    1,
    NULL,
    NULL,
    'Recursive CTE exceeded MAXRECURSION limit. Root cause: incorrect '
    + 'join condition in recursive member — pr.parent_referral_id joined '
    + 'to rc.parent_referral_id instead of rc.referral_id. '
    + 'Fixed by correcting join and adding explicit MAXRECURSION 50 guard.',
    'Scenario 7 — recursive CTE termination debugging'
);
GO


--STEP 8: Verify log entry
SELECT
    error_id,
    error_message,
    error_datetime,
    process_context
FROM error_log
WHERE process_context LIKE '%Scenario 7%';
GO


--STEP 9: Clean up
DROP TABLE patient_referrals;
GO


--========================================================================================================================
--DEBUGGING SCENARIO 8: Parameter Sniffing
/* Context: Parameter sniffing occurs when SQL Server compiles a stored procedure execution plan based on the first parameter values 
it sees. If those values are not representative of typical usage, the cached plan performs poorly for subsequent calls with different
parameters.

This is a common and difficult-to-diagnose issue in production — the query works fine in testing but performs poorly in production, 
or works Monday but is slow Tuesday. */

--STEP 1: Create a test table with skewed data distribution
--Skewed distribution is what makes parameter sniffing visible — one Admission_Type has vastly more rows than others, 
--so the optimal plan differs depending on which value is passed in.

CREATE TABLE admissions_sniff_test (
    admission_id        INT IDENTITY(1,1) PRIMARY KEY,
    Patient_Name        NVARCHAR(100),
    Medical_Condition   NVARCHAR(100),
    Admission_Type      NVARCHAR(50),
    Billing_Amount      DECIMAL(18,2),
    Admission_Date      DATE
);
GO

--Insert skewed data:
--Emergency = 95% of rows (common)
--Elective   = 4% of rows (rare)
--Urgent     = 1% of rows (very rare)

--Emergency rows — large volume
INSERT INTO admissions_sniff_test
    (Patient_Name, Medical_Condition, Admission_Type,
     Billing_Amount, Admission_Date)
SELECT TOP 9500
    'Patient ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS NVARCHAR),
    CASE ABS(CHECKSUM(NEWID())) % 3
        WHEN 0 THEN 'Cancer'
        WHEN 1 THEN 'Diabetes'
        ELSE 'Obesity'
    END,
    'Emergency',
    CAST(ABS(CHECKSUM(NEWID())) % 50000 + 5000 AS DECIMAL(18,2)),
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 365, GETDATE())
FROM sys.objects a CROSS JOIN sys.objects b;
GO

--Elective rows — small volume
INSERT INTO admissions_sniff_test
    (Patient_Name, Medical_Condition, Admission_Type,
     Billing_Amount, Admission_Date)
SELECT TOP 400
    'Patient ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS NVARCHAR),
    'Arthritis',
    'Elective',
    CAST(ABS(CHECKSUM(NEWID())) % 30000 + 3000 AS DECIMAL(18,2)),
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 365, GETDATE())
FROM sys.objects a CROSS JOIN sys.objects b;
GO

--Urgent rows — very small volume
INSERT INTO admissions_sniff_test
    (Patient_Name, Medical_Condition, Admission_Type,
     Billing_Amount, Admission_Date)
SELECT TOP 100
    'Patient ' + CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS NVARCHAR),
    'Asthma',
    'Urgent',
    CAST(ABS(CHECKSUM(NEWID())) % 20000 + 2000 AS DECIMAL(18,2)),
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 365, GETDATE())
FROM sys.objects a CROSS JOIN sys.objects b;
GO

--Create index on Admission_Type to make plan choice meaningful
CREATE NONCLUSTERED INDEX IX_sniff_admission_type
ON admissions_sniff_test (Admission_Type)
INCLUDE (Patient_Name, Medical_Condition, Billing_Amount, Admission_Date);
GO

--Verify distribution
SELECT
    Admission_Type,
    COUNT(*)                        AS row_count,
    CAST(COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER()
    AS DECIMAL(5,2))                AS pct_of_total
FROM admissions_sniff_test
GROUP BY Admission_Type;
GO


--STEP 2: Create a stored procedure vulnerable to sniffing
--Simple procedure filtering by Admission_Type. The execution plan compiled on first call will be cached and reused for all subsequent calls.

CREATE OR ALTER PROCEDURE usp_get_admissions_by_type
    @Admission_Type NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        Patient_Name,
        Medical_Condition,
        Admission_Type,
        Billing_Amount,
        Admission_Date
    FROM admissions_sniff_test
    WHERE Admission_Type = @Admission_Type
    ORDER BY Admission_Date DESC;
END;
GO


--STEP 3: Reproduce the problem
/* First call uses 'Urgent' — very few rows, SQL Server compiles a plan using an index seek (efficient for small result sets).

Second call uses 'Emergency' — 9500 rows. The cache index seek plan is now inefficient — a table scan would be better for large result 
sets. But SQL Server reuses the cached plan anyway. */


SET STATISTICS IO ON;
SET STATISTICS TIME ON;

--First execution — sniffs 'Urgent' parameter, compiles plan
EXEC usp_get_admissions_by_type @Admission_Type = 'Urgent';
GO

--Second execution — reuses 'Urgent' plan for 'Emergency'
--Watch logical reads — will be higher than optimal
EXEC usp_get_admissions_by_type @Admission_Type = 'Emergency';
GO

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


--STEP 4: Diagnose — check cached execution plans
--Queries the plan cache to find the compiled plan for this procedure and identify what parameter it was sniffed on.

SELECT
    qs.execution_count,
    qs.total_logical_reads,
    qs.total_worker_time            AS cpu_microseconds,
    qs.plan_generation_num,
    SUBSTRING(qt.text, 1, 300)      AS query_text,
    qp.query_plan
FROM sys.dm_exec_procedure_stats    ps
JOIN sys.dm_exec_query_stats        qs
    ON ps.plan_handle = qs.plan_handle
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle)     qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle)  qp
WHERE qt.text LIKE '%admissions_sniff_test%'
AND qt.text NOT LIKE '%dm_exec%'
ORDER BY qs.total_logical_reads DESC;
GO


--STEP 5: Diagnose — check statistics on the table
--Confirms the data distribution SQL Server uses when compiling plans. This shows why the sniffed plan is suboptimal for 
--Emergency queries.

DBCC SHOW_STATISTICS (
    'admissions_sniff_test',
    'IX_sniff_admission_type'
);
GO


--STEP 6: Fix option 1 — OPTION (RECOMPILE)
--Forces a fresh execution plan on every call.
--Eliminates sniffing entirely — each call gets the optimal plan for its specific parameter.
--Trade-off: compilation overhead on every execution. Best for: infrequently called procedures or where parameter distribution varies widely.

CREATE OR ALTER PROCEDURE usp_get_admissions_by_type_recompile
    @Admission_Type NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        Patient_Name,
        Medical_Condition,
        Admission_Type,
        Billing_Amount,
        Admission_Date
    FROM admissions_sniff_test
    WHERE Admission_Type = @Admission_Type
    ORDER BY Admission_Date DESC
    OPTION (RECOMPILE);             --recompile on every execution
END;
GO

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

EXEC usp_get_admissions_by_type_recompile @Admission_Type = 'Emergency';
GO

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


--STEP 7: Fix option 2 — OPTIMIZE FOR UNKNOWN
--Tells SQL Server to compile the plan using average
--statistics rather than the sniffed parameter value.
--More stable than sniffing, less overhead than RECOMPILE.
--Best for: frequently called procedures where compilation cost of RECOMPILE is too high.

CREATE OR ALTER PROCEDURE usp_get_admissions_by_type_optimized
    @Admission_Type NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        Patient_Name,
        Medical_Condition,
        Admission_Type,
        Billing_Amount,
        Admission_Date
    FROM admissions_sniff_test
    WHERE Admission_Type = @Admission_Type
    ORDER BY Admission_Date DESC
    OPTION (OPTIMIZE FOR (@Admission_Type UNKNOWN));
END;
GO

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

EXEC usp_get_admissions_by_type_optimized @Admission_Type = 'Emergency';
GO

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO


--STEP 8: Fix option 3 — clear the plan cache for this procedure
/*In production when sniffing is causing an immediate problem, clearing the cached plan forces recompilation on next execution. 
This is a quick fix, not a permanent solution — used while a proper fix is being deployed. */

DECLARE @plan_handle VARBINARY(64);

SELECT @plan_handle = qs.plan_handle
FROM sys.dm_exec_procedure_stats    ps
JOIN sys.dm_exec_query_stats        qs
    ON ps.plan_handle = qs.plan_handle
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
WHERE qt.text LIKE '%usp_get_admissions_by_type%'
AND qt.text NOT LIKE '%dm_exec%';

IF @plan_handle IS NOT NULL
BEGIN
    DBCC FREEPROCCACHE (@plan_handle);
    PRINT 'Plan cache cleared for usp_get_admissions_by_type';
END
ELSE
    PRINT 'No cached plan found';
GO


--STEP 9: Document the investigation

INSERT INTO error_log (
    error_number,
    error_severity,
    error_state,
    error_procedure,
    error_line,
    error_message,
    process_context
)
VALUES (
    0,
    0,
    0,
    'usp_get_admissions_by_type',
    NULL,
    'Parameter sniffing identified on usp_get_admissions_by_type. '
    + 'Procedure sniffed on Urgent parameter (100 rows) producing index seek plan. '
    + 'Plan reused for Emergency parameter (9500 rows) causing suboptimal execution. '
    + 'Fixed with OPTION(RECOMPILE) for ad-hoc reporting calls and '
    + 'OPTIMIZE FOR UNKNOWN for scheduled high-frequency executions.',
    'Scenario 8 — parameter sniffing investigation'
);
GO


--STEP 10: Verify log entry
SELECT
    error_id,
    error_message,
    error_datetime,
    process_context
FROM error_log
WHERE process_context LIKE '%Scenario 8%';
GO


--STEP 11: Clean up
DROP TABLE admissions_sniff_test;
DROP PROCEDURE usp_get_admissions_by_type;
DROP PROCEDURE usp_get_admissions_by_type_recompile;
DROP PROCEDURE usp_get_admissions_by_type_optimized;
GO



--View error log
SELECT *
FROM error_log
GO

