USE [healthcare_admissions]
GO

DROP TABLE IF EXISTS admissions_1;
DROP TABLE IF EXISTS admissions;



CREATE TABLE admissions_1 (
    Name                NVARCHAR(100),
    Age                 INT,
    Gender              VARCHAR(10),
    Blood_Type          NVARCHAR(5),
    Medical_Condition   VARCHAR(100),
    Date_of_Admission   NVARCHAR(15),
    Doctor              VARCHAR(100),
    Insurance_Provider  VARCHAR(100),
    Billing_Amount      DECIMAL(18,2),
    Room_Number         INT,
    Admission_Type      VARCHAR(50),
    Discharge_Date      NVARCHAR(15),
    Medication          VARCHAR(100),
    Test_Results        VARCHAR(50),
    Hospital_canonical  VARCHAR(100)

);
GO

BULK INSERT admissions_1
FROM '/var/opt/mssql/hospital_admissions.csv'
WITH (
    FIRSTROW = 2,          -- skip header row
    FIELDTERMINATOR = ';',
    ROWTERMINATOR = '\n',
    TABLOCK
);
GO

SELECT TOP 10 * FROM admissions_1 ;
GO

SELECT COUNT(*) AS total_rows FROM admissions_1;
GO

-- Convert Date_of_Admission from DDMMYY - YYMMDD - for better analysis later
ALTER TABLE admissions_1
ADD Admission_Date_clean DATE;
GO

UPDATE admissions_1
SET Admission_Date_clean = TRY_CONVERT(DATE, Date_of_Admission, 103)
GO

-- Convert Discharge_Date from DDMMYY - YYMMDD - for better analysis later
ALTER TABLE admissions_1
ADD Discharge_Date_clean DATE;
GO

UPDATE admissions_1
SET Discharge_Date_clean = TRY_CONVERT(DATE, Discharge_Date, 103)
GO

-- Drop col names to keep things clean 
ALTER TABLE admissions_1
DROP COLUMN Date_of_Admission;

ALTER TABLE admissions_1
DROP COLUMN Discharge_Date;

--check to see if the date changes have been implemented
SELECT TOP 10 * FROM admissions_1 ;
GO

-- ERROR LOG TABLE
-- Central logging table used across all debugging scenarios.
-- When a SQL process fails, error details are written here rather than just surfacing to the user 

DROP TABLE IF EXISTS error_log;

CREATE TABLE error_log (
    error_id            INT IDENTITY(1,1) PRIMARY KEY,
    error_number        INT,
    error_severity      INT,
    error_state         INT,
    error_procedure     NVARCHAR(200),
    error_line          INT,
    error_message       NVARCHAR(4000),
    error_datetime      DATETIME DEFAULT GETDATE(),
    process_context     NVARCHAR(200)   -- describes which process triggered the error
);
GO




