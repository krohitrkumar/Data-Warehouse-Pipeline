-- ========================================
-- 🏗 Bronze Layer Schema Definition
-- This script creates the raw data tables under the 'Bronze' schema.
-- Data is directly loaded here from source CSVs with minimal validation.
-- ========================================
CREATE SCHEMA IF NOT EXISTS Bronze;
CREATE SCHEMA IF NOT EXISTS Silver;
CREATE SCHEMA IF NOT EXISTS Gold;

CREATE TABLE IF NOT EXISTS Bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date TEXT  -- initially text for easier CSV import
);

CREATE TABLE IF NOT EXISTS Bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost VARCHAR(50),
    prd_line VARCHAR(50),
    prd_start_dt TEXT,
    prd_end_dt TEXT
);

CREATE TABLE IF NOT EXISTS Bronze.crm_sales_info (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

CREATE TABLE IF NOT EXISTS Bronze.erp_CUST_AZ12 (
    cid VARCHAR(50),
    bdate TEXT,
    gen VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Bronze.erp_LOC_A101 (
    cid VARCHAR(50),
    cntry VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Bronze.erp_PX_CAT_G1V2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);
