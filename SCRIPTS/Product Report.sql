/*
===================================================================================
							PRODUCT REPORT
===================================================================================
PURPOSE :
		 - This report consolidates key product metrics and behaviors

HIGHLIGHTS:

			1) Gathers essential fields such as product name, category, sub-category and cost details
			2) Segment products by revenue to identify high-performers, mid-range and low performers
			3) Aggregates product level metrics:
				- Total Orders
				- Total Sales
				- Total Quantity Sold
				- Total Consumers (unique)
				- Lifespan (in months)
			4) Calculate valuable KPI's:
				- Recency (months since last sale)
				- Average order revenue
				- Average monthly revenue

===================================================================================
*/
CREATE VIEW gold.report_products AS
WITH base_query AS(
/*---------------------------------------------------------------------------------
1) BASE QUERY - RETRIEVE CORE COLUMNS FROM FACT TABLE AND DIMENSION TABLE
-----------------------------------------------------------------------------------*/
SELECT  
S.order_number,
S.order_date,
S.customer_key,
S.sales_amount,
S.quantity,
P.product_key,
P.product_name,
P.category,
P.subcategory,
P.cost
FROM gold.fact_sales S
LEFT JOIN gold.dim_products P
ON S.product_key=P.product_key
WHERE order_date IS NOT NULL)

,product_aggregations AS(
/*---------------------------------------------------------------------------------
2) PRODUCT AGGREGATIONS - SUMMARIZES KEY METRICS AT THE PRODUCT LEVEL
-----------------------------------------------------------------------------------*/
SELECT 
product_key,
product_name,
category,
subcategory,
cost,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT customer_key) AS total_consumer,
MIN(order_date) AS first_sale_date,
MAX(order_date) AS last_sale_date,
DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
ROUND(AVG(CAST(sales_amount AS FLOAT)/NULLIF(quantity,0)),1) AS avg_selling_price
FROM base_query
GROUP BY
	product_key,
	product_name,
	category,
	subcategory,
	cost)

/*---------------------------------------------------------------------------------
3) FINAL QUERY - COMBINES ALL PRODUCT RESULTS IN ONE OUTPUT
-----------------------------------------------------------------------------------*/

SELECT 
product_key,
product_name,
category,
subcategory,
cost,
first_sale_date,
last_sale_date,
lifespan,
DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency,
CASE WHEN total_sales>50000 THEN 'High Performer'
	 WHEN total_sales >=10000 THEN 'Mid-Range'
	 ELSE 'Low Performer'
END AS product_segment,
total_orders,
total_sales,
total_quantity,
total_consumer,
avg_selling_price,
--AVERAGE ORDER REVENUE
CASE WHEN total_orders = 0 THEN 0
	 ELSE total_sales/total_orders
END AS avg_order_revenue,
--AVERAGE MONTHLY REVENUE
CASE WHEN lifespan = 0 THEN total_sales
	 ELSE total_sales/lifespan
END AS avg_monthly_revenue
FROM product_aggregations