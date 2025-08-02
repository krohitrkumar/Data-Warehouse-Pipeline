-- ========================================
-- Silver Layer Data Transformation Script
-- This script reads from Bronze tables, applies cleaning and type conversions,
-- then writes into Silver tables. Run after loadbronze.sql completes.
-- One-Time DATE Conversion for Bronze→Silver Sales Dates
-- This ensures sls_order_dt, sls_ship_dt, sls_due_dt are DATE.
-- ========================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'Silver'
          AND table_name   = 'crm_sales_info'
          AND data_type    != 'date'
          AND column_name IN ('sls_order_dt','sls_ship_dt','sls_due_dt')
    ) THEN
        ALTER TABLE Silver.crm_sales_info
          ALTER COLUMN sls_order_dt TYPE DATE USING TO_DATE(sls_order_dt::TEXT,'YYYYMMDD'),
          ALTER COLUMN sls_ship_dt  TYPE DATE USING TO_DATE(sls_ship_dt::TEXT,'YYYYMMDD'),
          ALTER COLUMN sls_due_dt   TYPE DATE USING TO_DATE(sls_due_dt::TEXT,'YYYYMMDD');
        RAISE NOTICE 'Sales date columns converted to DATE.';
    ELSE
        RAISE NOTICE 'Sales date columns already DATE – skipping conversion.';
    END IF;
END $$;


TRUNCATE TABLE Silver.crm_cust_info;

INSERT INTO Silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    TRIM(cst_key),
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'N/A'
    END,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'N/A'
    END,
    TO_DATE(cst_create_date, 'YYYY-MM-DD')
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
    FROM Bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) AS filtered
WHERE rn = 1;
DO $$
BEGIN
RAISE NOTICE ' CRM Customer data loaded into Silver';
END $$
TRUNCATE TABLE Silver.crm_prd_info;

INSERT INTO Silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    TRIM(prd_nm) AS prd_name,
    COALESCE(prd_cost::NUMERIC, 0) AS prd_cost,
    CASE 
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'N/A'
    END AS prd_line_desc,
    prd_start_dt AS prd_start_date,
    (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day') AS prd_end_date
FROM Bronze.crm_prd_info;

DO $$
BEGIN
RAISE NOTICE 'CRM Product data loaded into Silver';
END $$

TRUNCATE TABLE Silver.crm_sales_info;

INSERT INTO Silver.crm_sales_info (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    -- Order Date
    CASE 
        WHEN sls_order_dt::TEXT = '0' OR LENGTH(sls_order_dt::TEXT) <> 8 
            THEN NULL
        ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
    END AS sls_order_date,

    -- Ship Date
    CASE 
        WHEN sls_ship_dt::TEXT = '0' OR LENGTH(sls_ship_dt::TEXT) <> 8 
            THEN NULL
        ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
    END AS sls_ship_date,

    -- Due Date
    CASE 
        WHEN sls_due_dt::TEXT = '0' OR LENGTH(sls_due_dt::TEXT) <> 8 
            THEN NULL
        ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
    END AS sls_due_date,

    -- Sales Amount
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales_amt,

    sls_quantity,

    -- Price Amount
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price_amt

FROM Bronze.crm_sales_info;


DO $$
BEGIN
RAISE NOTICE 'CRM Sales data loaded into Silver';
END $$

TRUNCATE TABLE Silver.erp_cust_az12;

INSERT INTO Silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT 
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
        ELSE cid
    END,
    CASE 
        WHEN bdate > CURRENT_DATE THEN NULL
        ELSE TO_DATE(bdate, 'YYYY-MM-DD')
    END,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'N/A'
    END
FROM Bronze.erp_cust_az12;
DO $$
BEGIN
RAISE NOTICE ' ERP Customer AZ12 data loaded into Silver';
END $$

TRUNCATE TABLE Silver.erp_loc_a101;

INSERT INTO Silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT 
    REPLACE(cid, '-', ''),
    CASE 
        WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
        ELSE cntry
    END
FROM Bronze.erp_LOC_A101;
DO $$
BEGIN
RAISE NOTICE ' ERP Location data loaded into Silver';
END $$

TRUNCATE TABLE Silver.erp_px_cat_g1v2;

INSERT INTO Silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT 
    id,
    upper(TRIM(cat)),
    upper(TRIM(subcat)),
    upper(TRIM(maintenance))
FROM Bronze.erp_px_cat_g1v2;
DO $$
BEGIN
    RAISE NOTICE 'ERP PX Category data loaded into Silver';
END $$;
