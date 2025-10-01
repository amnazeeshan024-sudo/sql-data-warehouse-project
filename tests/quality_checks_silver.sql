
--------------CHK QUALITY OF CUST INFO TABLE-----------

--CHECK FOR NULL OR DUPLICATE  IN PRIMARY KEY
--EXPECTATION ERROR:NO RESULT
SELECT
cst_id,
count(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL



--CHECK FOR UNWANTED SPACESS
--EXPECTATION ERROR:NO RESULT
SELECT cst_firstname
from silver.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname)

--CHECK FOR UNWANTED SPACESS
--EXPECTATION ERROR:NO RESULT
SELECT cst_lastname 
from silver.crm_cust_info 
WHERE cst_lastname  != TRIM(cst_lastname )


--CHECK FOR UNWANTED SPACESS
--EXPECTATION ERROR:NO RESULT
SELECT cst_gndr
from silver.crm_cust_info 
WHERE cst_gndr != TRIM(cst_gndr)

--CHECK FOR UNWANTED SPACESS
--EXPECTATION ERROR:NO RESULT
SELECT cst_key
from silver.crm_cust_info 
WHERE cst_key != TRIM(cst_key)


--DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT cst_gndr
from silver.crm_cust_info 
select * from silver.crm_cust_info 


---------------CHK QUALITY OF PRD INFO TABLE----------
--CHECK FOR NULL OR DUPLICATE  IN PRIMARY KEY
--EXPECTATION ERROR:NO RESULT
SELECT
prd_id,
count(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--CHECK FOR UNWANTED SPACESS
--EXPECTATION ERROR:NO RESULT
SELECT prd_nm
from silver.crm_prd_info 
WHERE prd_nm != TRIM(prd_nm)

--CHECK FOR NULLS OR NEGATIVE NUMBERS 
--EXPECTATION ERROR:NO RESULT
SELECT prd_cost
from silver.crm_prd_info 
WHERE prd_cost < 0 OR prd_cost IS NULL 


--DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT prd_line
from silver.crm_prd_info 


--CHK FOR INVALID DATE ORDER
SELECT *
FROM silver.crm_prd_info 
WHERE prd_end_dt < prd_start_dt



---------------CRM SALES DETAIL------------
--CHECK FOR UNWANTED SPACESS
--EXPECTATION ERROR:NO RESULT
SELECT *
FROM silver.crm_sales_details
WHERE
sls_ord_num != TRIM(sls_ord_num)

----------------------
SELECT *
FROM silver.crm_sales_details
WHERE  sls_cust_id  NOT IN (SELECT cst_id FROM silver.crm_cust_info)


---------------------------

SELECT *
FROM silver.crm_sales_details
 WHERE  sls_prd_key  NOT IN (SELECT prd_key FROM silver.crm_prd_info)




  --------chk invalid date order----------
  SELECT *
  FROM silver.crm_sales_details
  WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

  -----chk data consistency : between sales,quantity,and price
  ---->> sales = quantity u8/ price
  -->> values must not be null,zero or negative.
  SELECT  DISTINCT
    sls_sales ,
    sls_quantity,
    sls_price 
    FROM silver.crm_sales_details
    WHERE  sls_sales != sls_quantity*sls_price 
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
        OR sls_sales <= 0  OR sls_quantity <= 0 OR sls_price <= 0
        ORDER BY sls_sales, sls_quantity,sls_price 
       

        --------------ERP FILES TABLES QUALITY CHK-------------
        --Identify out of range date---
SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12  
WHERE bdate < '1924-01-01' and bdate > GETDATE()


----DATA standardization & consistency 
SELECT DISTINCT 
gen
FROM silver.erp_cust_az12

----erp loc a101-----
SELECT DISTINCT 
cntry
FROM silver.erp_loc_a101
ORDER BY cntry

/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    prd_id,
    COUNT(*) 
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT 
    NULLIF(sls_due_dt, 0) AS sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
    OR LEN(sls_due_dt) != 8 
    OR sls_due_dt > 20500101 
    OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
