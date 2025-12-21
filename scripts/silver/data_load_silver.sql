/*
====================================================================================================
Loading Clean data to the Silver Layer (Bronze -> Silver)
====================================================================================================
Script Purpose:
    This script will load the clean data into 'silver' schema as stored procedure from the data in 'bronze' schema.
    It performs the following actions:
    - Truncates the tables before loading.
    - Uses the 'INSERT' Command to load the data from bronze tables to the tables.
	- Does some major Data transformation to transform the data in 'bronze' layer 
======================================================================================================
*/
-- Inserting data into the table
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@layer_start_time DATETIME,@layer_end_time DATETIME;
	SET @layer_start_time = GETDATE();
	PRINT'======================';
	PRINT'Loading a silver layer';
	PRINT'======================';

	PRINT'----------------------';
	PRINT'Loading CRM Tables';
	PRINT'----------------------';

	SET @start_time = GETDATE();
	-- Inserting a clean crm_cust_info data into silver.crm_cust_info table
	PRINT'-- Truncating the silver.crm_cust_info Table';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT'-- Inserting data into silver.crm_cust_info Table';
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
	SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname, -- Removed unwanted spaces
		TRIM(cst_lastname) AS cst_lastname, -- Removed unwanted spaces
		CASE
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			ELSE 'n/a'
		END AS cst_marital_status, -- Normalized the marital status values to readable format
		CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'n/a'
		END AS cst_gndr, -- Normalized the gender status values to readable format
		cst_create_date
	FROM(
		SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_duplicates
		FROM bronze.crm_cust_info)t 
	 WHERE flag_duplicates = 1; -- Finding the duplicates and the most recent record per customer
	SET @end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';

	-- Inserting a clean crm_prd_info data into silver.crm_prd_info table
	SET @start_time = GETDATE();
	PRINT'-- Truncating the silver.crm_prd_info Table';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT'-- Inserting data into silver.crm_prd_info Table';
	INSERT INTO silver.crm_prd_info(
		prd_id,
		prd_cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS prd_cat_id, -- Extract catagory id
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- Extract product id
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost, -- Handaling NULL values
		CASE 
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line, -- Mapping product line value with a readable descriptive values
		prd_start_dt,
		DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key  ORDER BY prd_start_dt)) AS prd_end_dt -- Calculate the end date as one day before the next start date
	FROM bronze.crm_prd_info;
	SET @end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';

	-- Inserting a clean crm_sales_details data into silver.crm_sales_details table
	SET @start_time = GETDATE();
	PRINT'-- Truncating the silver.crm_sales_details Table';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT'-- Inserting data into silver.crm_sales_details Table';
	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_quantity,
		sls_price,
		sls_sales
	)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CAST(CAST(CASE
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE sls_order_dt 
		END AS varchar)AS date) AS sls_order_dt, -- Changing the invalid data and data type
		CAST(CAST(CASE
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE sls_ship_dt 
		END AS varchar)AS date) AS sls_ship_dt, -- Changing the invalid data and data type
		CAST(CAST(CASE
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE sls_due_dt 
		END AS varchar)AS date) AS sls_due_dt, -- Changing the invalid data and data type
		sls_quantity,
		CASE 
			WHEN sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)
			WHEN sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price, -- Recalculating price if original data is missing or incorrect
		CASE
			WHEN sls_sales <= 0 THEN sls_quantity * ABS(sls_price)
			WHEN sls_sales IS NULL THEN sls_quantity * ABS(sls_price)
			WHEN sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales 
		END AS sls_sales -- Recalculating sales if original data is missing or incorrect
	FROM bronze.crm_sales_details;
	SET @end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';

	PRINT'----------------------';
	PRINT'Loading ERP Tables';
	PRINT'----------------------';

	-- Inserting a clean erp_cust_az12 data into silver.erp_cust_az12 table
	SET @start_time = GETDATE();
	PRINT'-- Truncating the silver.erp_cust_az12 Table';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT'-- Inserting data into silver.erp_cust_az12 Table';
	INSERT INTO silver.erp_cust_az12(
		CID,
		BDATE,
		GEN
	)
	SELECT 
		CASE
			WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
			ELSE CID 
		END AS CID,
		CASE 
			WHEN BDATE > GETDATE() THEN NULL
			ELSE BDATE
		END AS BDATE, -- Handeling invalid dates
		CASE 
			WHEN UPPER(TRIM(GEN)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(GEN)) = 'F' THEN 'Female'
			WHEN GEN IS NULL OR GEN = '' THEN 'n/a'
			ELSE TRIM(GEN)
		END AS GEN -- Changing invalid or incorrect values
	FROM bronze.erp_cust_az12;
	SET @end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';

	-- Inserting a clean erp_loc_a101 data into silver.erp_loc_a101 table
	SET @start_time = GETDATE();
	PRINT'-- Truncating the silver.erp_loc_a101 Table';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT'-- Inserting data into silver.erp_loc_a101 Table';
	INSERT INTO silver.erp_loc_a101(
		CID,
		CNTRY
	)
	SELECT
		REPLACE(CID,'-','') AS CID, -- Replacing '_' with '' to match the other table
		CASE
			WHEN TRIM(UPPER(CNTRY)) IN ('DE','GERMANY') THEN 'Germany'
			WHEN TRIM(UPPER(CNTRY)) IN ('USA','UNITED STATES','US') THEN 'United States of America'
			WHEN CNTRY IS NULL OR CNTRY = '' THEN 'n/a'
			ELSE TRIM(CNTRY)
		END AS CNTRY -- Correcting invalid or missing county code
	FROM bronze.erp_loc_a101;
	SET @end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';

	-- Inserting a clean erp_px_cat_g1v2 data into silver.erp_px_cat_g1v2 table
	SET @start_time = GETDATE();
	PRINT'-- Truncating the silver.erp_px_cat_g1v2 Table';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT'-- Inserting data into silver.erp_px_cat_g1v2 Table';
	INSERT INTO silver.erp_px_cat_g1v2(
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	)
	SELECT 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	FROM bronze.erp_px_cat_g1v2;
	SET @end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';

	SET @layer_end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration of Whole silver Layer: ' + CAST(DATEDIFF(second,@layer_start_time,@layer_end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';

END
