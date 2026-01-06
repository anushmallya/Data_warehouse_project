/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customer','V') IS NOT NULL
  DROP VIEW gold.dim_customer; -- Dropping the view if there is any view with this name
GO
  
CREATE VIEW gold.dim_customer AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cus_i.cst_id) AS customer_key,
	cus_i.cst_id AS customer_id,
	cus_i.cst_key AS customer_number,
	cus_i.cst_firstname AS first_name,
	cus_i.cst_lastname AS last_name,
	cus_i.cst_marital_status AS marital_status,
	cus_a1.CNTRY AS country,
	CASE
		WHEN cus_i.cst_gndr != 'n/a' THEN cus_i.cst_gndr
		ELSE COALESCE(cus_a2.GEN,'n/a')
	END AS gender,
	cus_a2.BDATE AS birth_date,
	cus_i.cst_create_date AS create_date
FROM silver.crm_cust_info AS cus_i
LEFT JOIN silver.erp_loc_a101 AS cus_a1
ON cus_i.cst_key = cus_a1.CID
LEFT JOIN silver.erp_cust_az12 AS cus_a2
ON cus_i.cst_key = cus_a2.CID
GO
-- =============================================================================
-- Create Dimension: gold.dim_product
-- =============================================================================
IF OBJECT_ID('gold.dim_product','V') IS NOT NULL
  DROP VIEW gold.dim_product; -- Dropping the view if there is any view with this name
GO

CREATE VIEW gold.dim_product AS
SELECT
	ROW_NUMBER() OVER(ORDER BY prd_1.prd_start_dt,prd_1.prd_id) AS product_key,
	prd_1.prd_id AS product_id,
	prd_1.prd_key AS product_number,
	prd_1.prd_nm AS product_name,
	prd_1.prd_cat_id AS catagory_id,
	prd_2.CAT AS catagory,
	prd_2.SUBCAT AS sub_catagory,
	prd_2.MAINTENANCE AS maintenance ,
	prd_1.prd_cost AS product_cost,
	prd_1.prd_line AS product_line,
	prd_1.prd_start_dt AS start_date
FROM silver.crm_prd_info AS prd_1
LEFT JOIN silver.erp_px_cat_g1v2 AS prd_2
ON prd_1.prd_cat_id = prd_2.ID
WHERE prd_1.prd_end_dt IS NULL -- Filter out all historical data
GO
-- =============================================================================
-- Create Fact: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales','V') IS NOT NULL
  DROP VIEW gold.fact_sales; -- Dropping the view if there is any view with this name
GO

CREATE VIEW gold.fact_sales AS
SELECT 
	sls_ord_num AS order_number,
	pr.product_key,
	cs.customer_key,
	sls_order_dt AS order_date,
	sls_ship_dt AS shipping_date,
	sls_due_dt AS due_date,
	sls_price AS price,
	sls_quantity AS quantity,
	sls_sales AS total_sales
FROM silver.crm_sales_details
LEFT JOIN gold.dim_product AS pr
ON sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer AS cs
ON sls_cust_id = cs.customer_id
GO
