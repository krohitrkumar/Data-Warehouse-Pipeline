-- ========================================
-- Gold Layer Verification Queries
-- This script runs quick checks against your Gold views.
-- ========================================

-- 1. Row counts for each view
SELECT 'dim_customers' AS view_name, COUNT(*) AS row_count FROM Gold.dim_customers;
SELECT 'dim_products'  AS view_name, COUNT(*) AS row_count FROM Gold.dim_products;
SELECT 'fact_sales'    AS view_name, COUNT(*) AS row_count FROM Gold.fact_sales;

-- 2. Sample records
-- Customers
SELECT * FROM Gold.dim_customers
ORDER BY customer_key
LIMIT 5;

-- Products
SELECT * FROM Gold.dim_products
ORDER BY product_key
LIMIT 5;

-- Sales
SELECT * FROM Gold.fact_sales
ORDER BY order_date DESC
LIMIT 5;

-- 3. Basic join sanity
-- Ensure every sale joins to a valid product and customer
SELECT
    COUNT(*) AS total_sales,
    COUNT(pr.product_key) AS matched_products,
    COUNT(cu.customer_key) AS matched_customers
FROM Silver.crm_sales_info sd
LEFT JOIN Gold.dim_products   pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN Gold.dim_customers  cu ON sd.sls_cust_id  = cu.customer_id;

-- 4. Null-check for key fields
SELECT
    SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS missing_customer_keys,
    SUM(CASE WHEN product_key  IS NULL THEN 1 ELSE 0 END) AS missing_product_keys
FROM Gold.fact_sales;
