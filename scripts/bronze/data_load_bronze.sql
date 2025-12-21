/*
====================================================================================================
Loading data to the Bronze Layer (Scource -> Bronze)
====================================================================================================
Script Purpose:
    This script will load the data into 'bronze' schema as stored procedure from external CSV File.
    It performs the following actions:
    - Truncates the tables before loading.
    - Uses the 'BULK INSERT' Command to load the data from csv file to the tables.
======================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@layer_start_time DATETIME,@layer_end_time DATETIME;
	SET @layer_start_time = GETDATE();
	PRINT'======================';
	PRINT'Loading a bronze layer';
	PRINT'======================';

	PRINT'----------------------';
	PRINT'Loading CRM Tables';
	PRINT'----------------------';

	SET @start_time = GETDATE();
	PRINT'-- Truncating the bronze.crm_cust_info Table';
	TRUNCATE TABLE bronze.crm_cust_info;
	PRINT'-- Inserting Data into bronze.crm_cust_info Table';
	BULK INSERT bronze.crm_cust_info
	FROM 'D:\SQL\sql-ultimate-course\Project\Data Warehousing Project\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';

	SET @start_time = GETDATE();
	PRINT'-- Truncating the bronze.crm_sales_details Table';
	TRUNCATE TABLE bronze.crm_sales_details;
	PRINT'-- Inserting Data into bronze.crm_sales_details Table';
	BULK INSERT bronze.crm_sales_details
	FROM 'D:\SQL\sql-ultimate-course\Project\Data Warehousing Project\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';
	
	SET @start_time = GETDATE();
	PRINT'-- Truncating the bronze.crm_prd_info Table';
	TRUNCATE TABLE bronze.crm_prd_info;
	PRINT'-- Inserting Data into bronze.crm_prd_info Table';
	BULK INSERT bronze.crm_prd_info
	FROM 'D:\SQL\sql-ultimate-course\Project\Data Warehousing Project\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';

	PRINT'----------------------';
	PRINT'Loading ERP Tables';
	PRINT'----------------------';
	
	SET @start_time = GETDATE();
	PRINT'-- Truncating the bronze.erp_cust_az12 Table';
	TRUNCATE TABLE bronze.erp_cust_az12;
	PRINT'-- Inserting Data into bronze.erp_cust_az12 Table';
	BULK INSERT bronze.erp_cust_az12
	FROM 'D:\SQL\sql-ultimate-course\Project\Data Warehousing Project\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';
	
	SET @start_time = GETDATE();
	PRINT'-- Truncating the bronze.erp_loc_a101 Table';
	TRUNCATE TABLE bronze.erp_loc_a101;
	PRINT'-- Inserting Data into bronze.erp_loc_a101 Table';
	BULK INSERT bronze.erp_loc_a101
	FROM 'D:\SQL\sql-ultimate-course\Project\Data Warehousing Project\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';
	
	SET @start_time = GETDATE();
	PRINT'-- Truncating the bronze.erp_px_cat_g1v2 Table';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	PRINT'-- Inserting Data into bronze.erp_px_cat_g1v2 Table';
	BULK INSERT bronze.erp_px_cat_g1v2
	FROM 'D:\SQL\sql-ultimate-course\Project\Data Warehousing Project\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';

	SET @layer_end_time = GETDATE();
	PRINT'------------------------------------------------------------------------------------------';
	PRINT'-- Load Duration of Whole Bronze Layer: ' + CAST(DATEDIFF(second,@layer_start_time,@layer_end_time) AS NVARCHAR) + ' seconds';
	PRINT'------------------------------------------------------------------------------------------';
END
