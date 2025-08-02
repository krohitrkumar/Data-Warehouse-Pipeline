-- ========================================
-- 🏗 Bronze Layer Data Loading Script
-- This script loads raw CSV data into Bronze schema tables.
-- Place CSVs under 01_datasets/source_crm and 01_datasets/source_erp.
-- ========================================

-- 1. Clear existing Bronze data
TRUNCATE TABLE Bronze.crm_sales_info;
TRUNCATE TABLE Bronze.crm_prd_info;
TRUNCATE TABLE Bronze.crm_cust_info;
TRUNCATE TABLE Bronze.erp_LOC_A101;
TRUNCATE TABLE Bronze.erp_CUST_AZ12;
TRUNCATE TABLE Bronze.erp_PX_CAT_G1V2;

DO $$
BEGIN
    RAISE NOTICE 'Bronze tables truncated – ready for fresh data load';
END $$;

-- 2. Load CRM CSVs
\copy Bronze.crm_sales_info FROM '01_datasets/source_crm/sales_details.csv' WITH (FORMAT csv, HEADER true);
\copy Bronze.crm_prd_info   FROM '01_datasets/source_crm/prd_info.csv'      WITH (FORMAT csv, HEADER true);
\copy Bronze.crm_cust_info  FROM '01_datasets/source_crm/cust_info.csv'     WITH (FORMAT csv, HEADER true);

-- 3. Load ERP CSVs
\copy Bronze.erp_LOC_A101     FROM '01_datasets/source_erp/LOC_A101.csv'      WITH (FORMAT csv, HEADER true);
\copy Bronze.erp_CUST_AZ12     FROM '01_datasets/source_erp/CUST_AZ12.csv'     WITH (FORMAT csv, HEADER true);
\copy Bronze.erp_PX_CAT_G1V2   FROM '01_datasets/source_erp/PX_CAT_G1V2.csv'   WITH (FORMAT csv, HEADER true);
