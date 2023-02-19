USE new_schema;

-- Total records = 532618
-- 134694 records have no CustomerID, 397924 records have CustomerID

CREATE TABLE online_retail_cleaned AS
WITH online_retail_filter1 AS
(
	SELECT * 
    FROM online_retail_c
	WHERE CustomerID != 0 AND Quantity > 0
)
    -- duplicate check
 ,dup_check AS
 (
	 SELECT *, ROW_NUMBER() OVER (PARTITION BY InvoiceNo, StockCode, Quantity ORDER BY InvoiceNo) AS Duplicate_flag
	 FROM online_retail_filter1
)
   -- 392702 clean data, 5217 duplicated records
SELECT * 
FROM dup_check
WHERE Duplicate_flag = 1;


-- BEGIN COHORT ANALSYT
SELECT * FROM online_retail_cleaned;

-- Data requires for cohort analysis: Unique identify(CustomerID), innitial start date(first InvoiceDate), revenue data
CREATE TABLE Cohort AS
SELECT CustomerID,
	   MIN(STR_TO_DATE(InvoiceDate, '%d/%m/%Y %H:%i')) AS First_purchase_date,
	   DATE_FORMAT(MIN(STR_TO_DATE(InvoiceDate, '%d/%m/%Y %H:%i')), '%Y-%m-01') AS Cohort_Date -- because im going to group the data per month and year
FROM online_retail_cleaned
GROUP BY CustomerID;       
       
-- We can do a retention analysis       
SELECT * FROM Cohort;       
        
-- Create Cohort Index: number of months have passed since the customer first engagement

CREATE TABLE Cohort_retention AS
SELECT mmm.*,
	   year_diff * 12 + month_diff + 1 AS Cohort_index -- Relize that we have 13 months for cohort index
FROM
(
	SELECT mm.*, 
		   Invoice_year - Cohort_year AS year_diff,
		   Invoice_month - Cohort_month AS month_diff
	FROM
	(
		SELECT o.*,
			   c.Cohort_Date,
			   YEAR(STR_TO_DATE(o.InvoiceDate, '%d/%m/%Y %H:%i')) AS Invoice_year,
			   MONTH(STR_TO_DATE(o.InvoiceDate, '%d/%m/%Y %H:%i')) AS Invoice_month,
			   YEAR(c.Cohort_Date) AS Cohort_year,
			   MONTH(c.Cohort_Date) AS Cohort_month
		FROM online_retail_cleaned o LEFT JOIN Cohort c ON o.CustomerID = c.CustomerID
	) mm
)mmm;


SELECT * FROM Cohort_retention; -- export file for visualization in tableau


-- Pivot data to see the cohort
CREATE TABLE Cohort_pivot AS
	SELECT Cohort_Date,
		   COUNT(CASE WHEN Cohort_index = 1 THEN CustomerID END) AS 'Cohort_1',
		   COUNT(CASE WHEN Cohort_index = 2 THEN CustomerID END) AS 'Cohort_2',
		   COUNT(CASE WHEN Cohort_index = 3 THEN CustomerID END) AS 'Cohort_3',
		   COUNT(CASE WHEN Cohort_index = 4 THEN CustomerID END) AS 'Cohort_4',
		   COUNT(CASE WHEN Cohort_index = 5 THEN CustomerID END) AS 'Cohort_5',
		   COUNT(CASE WHEN Cohort_index = 6 THEN CustomerID END) AS 'Cohort_6',
		   COUNT(CASE WHEN Cohort_index = 7 THEN CustomerID END) AS 'Cohort_7',
		   COUNT(CASE WHEN Cohort_index = 8 THEN CustomerID END) AS 'Cohort_8',
		   COUNT(CASE WHEN Cohort_index = 9 THEN CustomerID END) AS 'Cohort_9',
		   COUNT(CASE WHEN Cohort_index = 10 THEN CustomerID END) AS 'Cohort_10',
		   COUNT(CASE WHEN Cohort_index = 11 THEN CustomerID END) AS 'Cohort_11',
		   COUNT(CASE WHEN Cohort_index = 12 THEN CustomerID END) AS 'Cohort_12',
		   COUNT(CASE WHEN Cohort_index = 13 THEN CustomerID END) AS 'Cohort_13'
	FROM
	(
	-- Find the unique customer
		SELECT DISTINCT CustomerID,
			   Cohort_Date,
			   Cohort_index
		FROM Cohort_retention
	) tbl
	GROUP BY Cohort_Date;

SELECT * FROM Cohort_pivot; -- Turn numberic to percentage ratio

SELECT
    1.0 * Cohort_1/Cohort_1 * 100 AS CR1,
    1.0 * Cohort_2/Cohort_1 * 100 AS CR2,
    1.0 * Cohort_3/Cohort_1 * 100 AS CR3,
    1.0 * Cohort_4/Cohort_1 * 100 AS CR4,
    1.0 * Cohort_5/Cohort_1 * 100 AS CR5,
    1.0 * Cohort_6/Cohort_1 * 100 AS CR6,
    1.0 * Cohort_7/Cohort_1 * 100 AS CR7,
    1.0 * Cohort_8/Cohort_1 * 100 AS CR8,
    1.0 * Cohort_9/Cohort_1 * 100 AS CR9,
    1.0 * Cohort_10/Cohort_1 * 100 AS CR10,
    1.0 * Cohort_11/Cohort_1 * 100 AS CR11,
    1.0 * Cohort_12/Cohort_1 * 100 AS CR12,
    1.0 * Cohort_13/Cohort_1 * 100 AS CR13
FROM Cohort_pivot;
