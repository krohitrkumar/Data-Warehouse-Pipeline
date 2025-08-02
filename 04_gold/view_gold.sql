-- ========================================
--  Gold Layer View Definitions
-- This script defines analytical dimension and fact views under the 'Gold' schema.
-- Run after Silver transformations complete.
-- ========================================
CREATE SCHEMA IF NOT EXISTS Gold;

--customer
DROP VIEW IF EXISTS gold.dim_customers;

CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
        ELSE COALESCE(co.gen, 'N/A')
    END AS gender,
    ci.cst_create_date AS create_date, 
    co.bdate AS birthdate,
    lo.cntry AS country
FROM silver.crm_cust_info AS ci 
LEFT JOIN silver.erp_cust_az12 AS co
    ON (ci.cst_key) = (co.cid)
LEFT JOIN silver.erp_loc_a101 AS lo
    ON (ci.cst_key) = (lo.cid);

SELECT * FROM gold.dim_customers;
--product
DROP VIEW IF EXISTS gold.dim_products;

CREATE VIEW gold.dim_products AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
  pn.prd_id AS product_id,
  pn.prd_key AS product_number,
  pn.prd_nm AS product_name,
  pn.cat_id AS category_id,
  COALESCE(pc.cat, 'UNKNOWN') AS category,
  COALESCE(pc.subcat, 'UNKNOWN') AS subcategory,
  COALESCE(pc.maintenance, 'UNKNOWN') AS maintenance,
  pn.prd_cost AS cost,
  pn.prd_line AS product_line,
  pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
  ON TRIM(pn.cat_id) = TRIM(pc.id)
WHERE pn.prd_end_dt IS NULL;

select * from gold.dim_customers;
select * from  gold.dim_products;
SELECT * from silver.crm_sales_info;
-- Fact sales
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
select 
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key, 
	sd.sls_order_dt as order_date, 
	sd.sls_ship_dt as ship_date, 
	sd.sls_due_dt as due_date, 
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from silver.crm_sales_info sd
LEFT join gold.dim_products pr 
on sd.sls_prd_key = pr.product_number
LEFT join gold.dim_customers cu 
on sd.sls_cust_id = cu.customer_id;


DO $$
BEGIN
    RAISE NOTICE '=== ETL PIPELINE COMPLETED SUCCESSFULLY ===';
    RAISE NOTICE 'All data loaded and transformed across Bronze → Silver → Gold layers';
END $$;
