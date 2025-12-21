/*
=====================================================================================================================================
Quality Checks
=====================================================================================================================================
Script Purpose:
  This script is used to perform a quality check for data consistancy, accuracy, and standardization across the 'silver' schema

Usage:
  - Run these checks after data loaded in silver layer
  - Check and resolve if there is any quality issue in the data
=====================================================================================================================================
*/
=====================================
-- Checking silver.crm_cust_info
=====================================
SELECT *
FROM silver.crm_cust_info

-- First check for Nulls or Duplicates in Primary Key

SELECT 
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL

-- Check for unwanted spaces
SELECT *
FROM
	(SELECT 
		cst_firstname,
		TRIM(cst_firstname) AS trimed_first_name
	FROM silver.crm_cust_info
	)t
WHERE cst_firstname != trimed_first_name

SELECT *
FROM
	(SELECT 
		cst_lastname,
		TRIM(cst_lastname) AS trimed_last_name
	FROM silver.crm_cust_info
	)t
WHERE cst_lastname != trimed_last_name

SELECT *
FROM
	(SELECT 
		cst_marital_status,
		TRIM(cst_marital_status) AS trimed_marital_status
	FROM silver.crm_cust_info
	)t
WHERE cst_marital_status != trimed_marital_status

SELECT *
FROM
	(SELECT 
		cst_gndr,
		TRIM(cst_gndr) AS trimed_gender
	FROM silver.crm_cust_info
	)t
WHERE cst_gndr != trimed_gender

-- Data Standardizarion and Consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info
=================================
-- Checking silver.crm_prd_info
=================================
SELECT *
FROM silver.crm_prd_info

-- First check Duplicates in Primary Key

SELECT 
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for any unwanted blanck spaces

SELECT
	prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


-- Check for Negetive or NULL Values

SELECT 
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


-- Check the invalid date

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt
======================================
-- Checking silver.crm_sales_details
======================================
SELECT *
FROM silver.crm_sales_details

-- Check if the sls_cust_id and is in a connected table
SELECT 
*
FROM silver.crm_sales_details
WHERE 
	sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info) OR 
	sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info) OR 
	sls_ord_num != TRIM(sls_ord_num)

-- Check Invalid Dates

SELECT 
	sls_order_dt
FROM silver.crm_sales_details
WHERE 
	sls_order_dt <= 0 OR 
	LEN(sls_order_dt) != 8 OR
	sls_order_dt > 20500101 OR
	sls_order_dt < 19000101

SELECT 
	sls_ship_dt
FROM silver.crm_sales_details
WHERE 
	sls_ship_dt <= 0 OR 
	LEN(sls_ship_dt) != 8 OR
	sls_ship_dt > 20500101 OR
	sls_ship_dt < 19000101

SELECT 
	sls_due_dt
FROM silver.crm_sales_details
WHERE 
	sls_due_dt <= 0 OR 
	LEN(sls_due_dt) != 8 OR
	sls_due_dt > 20500101 OR
	sls_due_dt < 19000101

SELECT 
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt OR sls_ship_dt > sls_due_dt

-- Check valid sales

SELECT DISTINCT
	sls_quantity,
	sls_price,
	sls_sales
FROM silver.crm_sales_details
WHERE 
	sls_sales != sls_quantity * sls_price OR
	sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL OR
	sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_quantity,sls_price,sls_sales
==================================
-- Checking silver.erp_cust_az12
==================================
SELECT * FROM silver.erp_cust_az12

-- Check CID to convert it to cut_key

SELECT DISTINCT
	SUBSTRING(CID,1,5) AS CID,
	COUNT(*)
FROM silver.erp_cust_az12
GROUP BY SUBSTRING(CID,1,5)

-- Check for invalid bith Dates OR Null Values

SELECT 
	BDATE
FROM silver.erp_cust_az12
WHERE BDATE > GETDATE()

-- Check for missing, incorrect, unwanted space or null values in gender

SELECT DISTINCT GEN
FROM silver.erp_cust_az12

SELECT GEN
FROM silver.erp_cust_az12
WHERE GEN != TRIM(GEN)
=================================
-- Checking silver.erp_loc_a101
=================================
SELECT * FROM silver.erp_loc_a101

-- Check if the cid data match with other table to join 
SELECT 
	CID
FROM silver.erp_loc_a101
WHERE CID NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- Check for unwanted space , null, incorrect values in CNTRY 

SELECT DISTINCT 
	CNTRY
FROM silver.erp_loc_a101
====================================
-- Checking silver.erp_px_cat_g1v2
====================================
SELECT * FROM silver.erp_px_cat_g1v2

-- Check if ID has missing or matched OR Null values in other table
SELECT ID FROM silver.erp_px_cat_g1v2
WHERE ID NOT IN (SELECT prd_cat_id FROM silver.crm_prd_info) OR ID IS NULL

SELECT CAT
FROM silver.erp_px_cat_g1v2
WHERE CAT != TRIM(CAT)

SELECT DISTINCT SUBCAT
FROM silver.erp_px_cat_g1v2
WHERE SUBCAT != TRIM(SUBCAT)

SELECT DISTINCT MAINTENANCE
FROM silver.erp_px_cat_g1v2
WHERE MAINTENANCE != TRIM(MAINTENANCE)
