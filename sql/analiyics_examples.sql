-- ANALYTICS EXAMPLES
-- Data Warehouse - Gold Layer
-- 
-- These queries demonstrate the dimensional model usability.


-- 1. Total Revenue
SELECT
    SUM(total_amount) AS total_revenue
FROM gold.fact_sales;


-- 2. Revenue by Month
SELECT
    d.year,
    d.month,
    SUM(f.total_amount) AS monthly_revenue
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;


-- 3. Top 5 Products by Revenue
SELECT
    p.title,
    SUM(f.total_amount) AS revenue
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.title
ORDER BY revenue DESC
LIMIT 5;


-- 4. Revenue by Category
SELECT
    p.category,
    SUM(f.total_amount) AS revenue
FROM gold.fact_sales f
JOIN gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY revenue DESC;


-- 5. Average Ticket per User
SELECT
    u.username,
    SUM(f.total_amount) AS total_spent,
    COUNT(*) AS number_of_purchases,
    ROUND(SUM(f.total_amount) / COUNT(*), 2) AS avg_ticket
FROM gold.fact_sales f
JOIN gold.dim_user u ON f.user_key = u.user_key
GROUP BY u.username
ORDER BY total_spent DESC;


-- 6. Daily Sales Trend
SELECT
    d.date_key,
    SUM(f.total_amount) AS daily_revenue
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.date_key
ORDER BY d.date_key;
