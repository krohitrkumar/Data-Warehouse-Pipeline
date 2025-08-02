-- ========================================
-- Global Error & Data Quality Checks
-- This script runs across Bronze, Silver, and Gold layers to surface anomalies.
-- ========================================

-- ========== Bronze Layer Checks ==========

-- 1. NULL or duplicate customer IDs
SELECT 'Bronze.crm_cust_info' AS object, COUNT(*) AS error_count
FROM Bronze.crm_cust_info
WHERE cst_id IS NULL
   OR cst_id IN (
     SELECT cst_id
     FROM Bronze.crm_cust_info
     GROUP BY cst_id
     HAVING COUNT(*) > 1
   );

-- 2. Leading/trailing spaces in customer first names
SELECT 'Bronze.crm_cust_info' AS object, COUNT(*) AS error_count
FROM Bronze.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname);

-- 3. Invalid gender values
SELECT 'Bronze.crm_cust_info' AS object, COUNT(*) AS error_count
FROM Bronze.crm_cust_info
WHERE UPPER(TRIM(cst_gndr)) NOT IN ('M','F');

-- 4. Leading/trailing spaces in product names
SELECT 'Bronze.crm_prd_info' AS object, COUNT(*) AS error_count
FROM Bronze.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);

-- 5. Invalid product lines
SELECT 'Bronze.crm_prd_info' AS object, COUNT(*) AS error_count
FROM Bronze.crm_prd_info
WHERE UPPER(TRIM(prd_line)) NOT IN ('M','R','S','T');

-- 6. NULL or negative product cost
SELECT 'Bronze.crm_prd_info' AS object, COUNT(*) AS error_count
FROM Bronze.crm_prd_info
WHERE prd_cost IS NULL OR CAST(prd_cost AS DECIMAL) < 0;

-- 7. Invalid product date range
SELECT 'Bronze.crm_prd_info' AS object, COUNT(*) AS error_count
FROM Bronze.crm_prd_info
WHERE TO_DATE(prd_start_dt,'YYYY-MM-DD') > TO_DATE(prd_end_dt,'YYYY-MM-DD');

-- 8. Invalid sales order dates
SELECT 'Bronze.crm_sales_info' AS object, COUNT(*) AS error_count
FROM Bronze.crm_sales_info
WHERE sls_order_dt IS NULL
   OR sls_order_dt <= 0
   OR LENGTH(sls_order_dt::TEXT) <> 8
   OR sls_order_dt > 20500101
   OR sls_order_dt < 19000101;

-- 9. Inconsistent sales calculation
SELECT 'Bronze.crm_sales_info' AS object, COUNT(*) AS error_count
FROM Bronze.crm_sales_info
WHERE sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <> sls_quantity * ABS(sls_price)
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0;

-- 10. ERP gender standardization errors
SELECT 'Bronze.erp_cust_az12' AS object, COUNT(*) AS error_count
FROM Bronze.erp_cust_az12
WHERE UPPER(TRIM(gen)) NOT IN ('M','F','MALE','FEMALE');

-- 11. ERP location standardization errors
SELECT 'Bronze.erp_loc_a101' AS object, COUNT(*) AS error_count
FROM Bronze.erp_loc_a101
WHERE TRIM(cntry) = '' OR cntry IS NULL;

-- 12. ERP category trimming errors
SELECT 'Bronze.erp_px_cat_g1v2' AS object, COUNT(*) AS error_count
FROM Bronze.erp_px_cat_g1v2
WHERE cat <> TRIM(cat)
   OR subcat <> TRIM(subcat)
   OR maintenance <> TRIM(maintenance);

-- ========== Silver Layer Checks ==========

-- 1. NULL or duplicate customer IDs
SELECT 'Silver.crm_cust_info' AS object, COUNT(*) AS error_count
FROM Silver.crm_cust_info
WHERE cst_id IS NULL
   OR cst_id IN (
     SELECT cst_id
     FROM Silver.crm_cust_info
     GROUP BY cst_id
     HAVING COUNT(*) > 1
   );

-- 2. Invalid dates in Silver.crm_cust_info
SELECT 'Silver.crm_cust_info' AS object, COUNT(*) AS error_count
FROM Silver.crm_cust_info
WHERE cst_create_date IS NULL;

-- 3. NULL or duplicate product IDs
SELECT 'Silver.crm_prd_info' AS object, COUNT(*) AS error_count
FROM Silver.crm_prd_info
WHERE prd_id IS NULL
   OR prd_id IN (
     SELECT prd_id
     FROM Silver.crm_prd_info
     GROUP BY prd_id
     HAVING COUNT(*) > 1
   );

-- 4. Negative product cost in Silver
SELECT 'Silver.crm_prd_info' AS object, COUNT(*) AS error_count
FROM Silver.crm_prd_info
WHERE prd_cost < 0;

-- 5. Date logic in Silver.crm_prd_info
SELECT 'Silver.crm_prd_info' AS object, COUNT(*) AS error_count
FROM Silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

-- 6. NULL or duplicate order numbers
SELECT 'Silver.crm_sales_info' AS object, COUNT(*) AS error_count
FROM Silver.crm_sales_info
WHERE sls_ord_num IS NULL
   OR sls_ord_num IN (
     SELECT sls_ord_num
     FROM Silver.crm_sales_info
     GROUP BY sls_ord_num
     HAVING COUNT(*) > 1
   );

-- 7. Negative or zero sales in Silver
SELECT 'Silver.crm_sales_info' AS object, COUNT(*) AS error_count
FROM Silver.crm_sales_info
WHERE sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0;

-- 8. ERP Customer AZ12 date issues in Silver
SELECT 'Silver.erp_cust_az12' AS object, COUNT(*) AS error_count
FROM Silver.erp_cust_az12
WHERE bdate IS NULL;

-- 9. ERP Location country standardization in Silver
SELECT 'Silver.erp_loc_a101' AS object, COUNT(*) AS error_count
FROM Silver.erp_loc_a101
WHERE cntry = 'N/A';

-- 10. ERP PX Category uppercase errors in Silver
SELECT 'Silver.erp_px_cat_g1v2' AS object, COUNT(*) AS error_count
FROM Silver.erp_px_cat_g1v2
WHERE cat   <> UPPER(TRIM(cat))
   OR subcat<> UPPER(TRIM(subcat))
   OR maintenance <> UPPER(TRIM(maintenance));

-- ========== Gold Layer Checks ==========

-- 1. Row count sanity for Gold.dim_customers
SELECT 'Gold.dim_customers' AS object, COUNT(*) AS row_count
FROM Gold.dim_customers;

-- 2. Row count sanity for Gold.dim_products
SELECT 'Gold.dim_products' AS object, COUNT(*) AS row_count
FROM Gold.dim_products;

-- 3. Row count sanity for Gold.fact_sales
SELECT 'Gold.fact_sales' AS object, COUNT(*) AS row_count
FROM Gold.fact_sales;

-- 4. Orphan fact_sales entries
SELECT 'Gold.fact_sales' AS object, COUNT(*) AS error_count
FROM Gold.fact_sales fs
LEFT JOIN Gold.dim_customers cu ON fs.customer_key = cu.customer_key
LEFT JOIN Gold.dim_products  pr ON fs.product_key  = pr.product_key
WHERE cu.customer_key IS NULL
   OR pr.product_key IS NULL;

-- 5. Null key checks in Gold.fact_sales
SELECT 'Gold.fact_sales' AS object, 
       SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) +
       SUM(CASE WHEN product_key  IS NULL THEN 1 ELSE 0 END) 
       AS error_count
FROM Gold.fact_sales;
