---- CRM CUST INFO TABLE
--CHECK FOR NULL OR DUPLICATE  IN PRIMARY KEY
--EXPECTATION ERROR:NO RESULT
SELECT
cst_id,
count(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


--CHECK FOR UNWANTED SPACESS
--EXPECTATION ERROR:NO RESULT
SELECT cst_firstname
from bronze.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname)

--CHECK FOR UNWANTED SPACESS
--EXPECTATION ERROR:NO RESULT
SELECT cst_lastname 
from bronze.crm_cust_info 
WHERE cst_lastname  != TRIM(cst_lastname )


--CHECK FOR UNWANTED SPACESS
--EXPECTATION ERROR:NO RESULT
SELECT cst_gndr
from bronze.crm_cust_info 
WHERE cst_gndr != TRIM(cst_gndr)

--CHECK FOR UNWANTED SPACESS
--EXPECTATION ERROR:NO RESULT
SELECT cst_key
from bronze.crm_cust_info 
WHERE cst_key != TRIM(cst_key)


--DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT cst_gndr
from bronze.crm_cust_info 


   --------------------CRM PROD INFO TABLE-------------

   --CHECK FOR NULL OR DUPLICATE  IN PRIMARY KEY
--EXPECTATION ERROR:NO RESULT
SELECT
prd_id,
count(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL
--CHECK FOR UNWANTED SPACESS
--EXPECTATION ERROR:NO RESULT
SELECT prd_nm
from bronze.crm_prd_info 
WHERE prd_nm != TRIM(prd_nm)

--CHECK FOR NULLS OR NEGATIVE NUMBERS 
--EXPECTATION ERROR:NO RESULT
SELECT prd_cost
from bronze.crm_prd_info 
WHERE prd_cost < 0 OR prd_cost IS NULL 


--DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT prd_line
from bronze.crm_prd_info 


--CHK FOR INVALID DATE ORDER
SELECT *
FROM bronze.crm_prd_info 
WHERE prd_end_dt < prd_start_dt



--------------CRM SALES DETAIL----
--CHECK FOR UNWANTED SPACESS
--EXPECTATION ERROR:NO RESULT
SELECT *
FROM bronze.crm_sales_details
WHERE  sls_ord_num != TRIM(sls_ord_num)

----------------------
SELECT *
FROM bronze.crm_sales_details
WHERE  sls_cust_id  NOT IN (SELECT cst_id FROM silver.crm_cust_info)


---------------------------

SELECT *
FROM bronze.crm_sales_details
 WHERE  sls_prd_key  NOT IN (SELECT prd_key FROM silver.crm_prd_info)

 -----------check for invalid date-------
 SELECT 
NULLIF(sls_order_dt ,0) sls_order_dt
 FROM bronze.crm_sales_details
 WHERE sls_order_dt <= 0 
 OR LEN(sls_order_dt)!=8
 OR sls_order_dt > 20500101
  OR sls_order_dt < 19000101


  -----------check for invalid date-------
 SELECT 
NULLIF(sls_ship_dt ,0) sls_order_dt
 FROM bronze.crm_sales_details
 WHERE sls_ship_dt <= 0 
 OR LEN(sls_ship_dt)!=8
 OR sls_ship_dt > 20500101
  OR sls_ship_dt < 19000101


   -----------check for invalid date-------
 SELECT 
NULLIF(sls_due_dt ,0) sls_order_dt
 FROM bronze.crm_sales_details
 WHERE sls_due_dt <= 0 
 OR LEN(sls_due_dt)!=8
 OR sls_due_dt > 20500101
  OR sls_due_dt < 19000101


  --------chk invalid date order----------
  SELECT *
  FROM bronze.crm_sales_details
  WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


  -----chk data consistency : between sales,quantity,and price
  ---->> sales = quantity u8/ price
  -->> values must not be null,zero or negative.


  SELECT  DISTINCT
    sls_sales ,
    sls_quantity,
    sls_price 
    FROM bronze.crm_sales_details
    WHERE  sls_sales != sls_quantity*sls_price 
    OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
        OR sls_sales <= 0  OR sls_quantity <= 0 OR sls_price <= 0
        ORDER BY sls_sales, sls_quantity,sls_price 

        -------------ERP FILES TABLES QUALITY CHK-------------

--Identify out of range date---
SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12  
WHERE bdate < '1924-01-01' and bdate > GETDATE()


----DATA standardization & consistency 
SELECT DISTINCT 
gen

FROM bronze.erp_cust_az12


----------erp table ---------
 --replcng - wth underscore chk cst key from slver crm cust info match wth cd n bronze layerii
 
 
 SELECT 
     REPLACE (cid,'-','') cid,
      cntry
     FROM bronze.erp_loc_a101 WHERE   REPLACE (cid,'-','')  NOT IN
     (SELECT cst_key FROM silver.crm_cust_info)
    

    ----DATA standardization & consistency 
    SELECT DISTINCT cntry 
    FROM bronze.erp_loc_a101
    ORDER BY cntry


    ---erp pxcat g1v2----
    Select 
    id,
    cat,
    subcat,
    maintenance
    FROM bronze.erp_px_cat_g1v2
    --chk for unwanted spaces---
    select * from bronze.erp_px_cat_g1v2
    where cat != TRiM(cat) OR subcat != TRiM(subcat) OR  maintenance != TRiM( maintenance) 
    
    ----DATA standardization & consistency ---------
      SELECT DISTINCT
      id,
      cat ,
     subcat ,
       maintenance
      FROM bronze.erp_px_cat_g1v2
