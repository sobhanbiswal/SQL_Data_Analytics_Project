/*SQL Tasks*/

-- Analyze Sales over Time

use DataWarehouseAnalytics;

select * from gold.fact_sales;

-- Yearly Analysis
select Year(order_date) order_year,
	   Sum(sales_amount) yearly_sales,
	   COUNT(distinct customer_key) total_customer_by_year,
	   SUM(quantity) yearly_quantity
from gold.fact_sales
where order_date is not null
group by Year(order_date)
order by Year(order_date);

-- Monthly Analysis
select YEAR(order_date) order_year,
	   DATENAME(month,order_date) month_name, 
	   MONTH(order_date) order_month,
	   SUM(sales_amount) monthly_sales,
	   COUNT(distinct customer_key) total_customer_by_month,
	   SUM(quantity) monthly_quantity
from gold.fact_sales
where order_date is not null
group by year(order_date),
		 DATENAME(month,order_date),
		 MONTH(order_date)
order by year(order_date),
		 MONTH(order_date);

-- or

select YEAR(order_date) order_year,
	   FORMAT(order_date, 'MMMM') AS month_name, 
	   MONTH(order_date) order_month,
	   SUM(sales_amount) monthly_sales,
	   COUNT(distinct customer_key) total_customer_by_month,
	   SUM(quantity) monthly_quantity
from gold.fact_sales
where order_date is not null
group by year(order_date),
		 FORMAT(order_date, 'MMMM'),
		 MONTH(order_date)
order by year(order_date),
		 MONTH(order_date);

--or

/*APPLICABLE FOR latest version---
select datetrunc(month, order_date),    -- grouped by month but shows full date
	   SUM(sales_amount) monthly_sales,
	   count(distinct customer_key) total_quantity_by_month,
	   sum(quantity) monthly_quantity
from gold.fact_sales
where order_date is not null
group by datetrunc(month, order_date),
order by datetrunc(month, order_date) ;
*/

-- Cumulative Analysis  - Aggregating data progressively over time   Shows progress of business over the year
-- Running total sales over year and moving average sales by month
-- Calculate the total sales over month and
-- running total of sales over time

select
order_date_monthly,
sales_amount,
SUM(sales_amount) over (order by year) as running_total_yearly,
SUM(sales_amount) over (order by year,month) as running_total_monthly,
SUM(sales_amount) over (partition by year order by month) running_total_monthly_limiting_per_year

from(
		select 
		YEAR(order_date) year,
		MONTH(order_date) month,
		FORMAT(order_date, 'yyyy-MMM') order_date_monthly,
		SUM(sales_amount) sales_amount
		from gold.fact_sales
		where order_date is not null
		group by
			YEAR(order_date),
			MONTH(order_date), 
			FORMAT(order_date, 'yyyy-MMM')
		
	) t
order by year, month;

-- Running total over month limiting year
select 
order_date,
sales_amount,
SUM(sales_amount) over (partition by year order by month) running_total_monthly_limiting_per_year
from(
		select 
		YEAR(order_date) year,
		MONTH(order_date) month,
		FORMAT(order_date, 'yyyy-MMMM') order_date,
		SUM(sales_amount) sales_amount
		from gold.fact_sales
		where order_date is not null
		group by
				YEAR(order_date),
				MONTH(order_date),
				FORMAT(order_date, 'yyyy-MMMM')
	) t
	
order by year,month
;



--Running total over year
select
year,
sales_amount,
SUM(sales_amount) over (order by year) as running_total_yearly,
AVG(average) over(order by year) as moving_average
from(
		select 
		YEAR(order_date) year,
		SUM(sales_amount) sales_amount,
		AVG(sales_amount) average
		from gold.fact_sales
		where order_date is not null
		group by
			YEAR(order_date)
		
	) t
order by year;


-- Performance Analysis  - Compare the current value with target value   done by difference of current[measure] - target[measure]
																							 --current[sale] - average[sale]
																							 --current year sale - previous year sale	-> YOY analysis 
																							 --current[sale] - lowest[sale]

/*Analysis of yearly performance of products by comparing 
each products sales to both its average sales performance and the previous year sales*/

with year_product_sales as (
select 
year(s.order_date) as order_year,
p.product_name,
sum(s.sales_amount) as current_sales
from gold.fact_sales s
left join gold.dim_products p
on s.product_key=p.product_key
where s.order_date is not null
group by year(s.order_date),p.product_name)

select 
order_year,
product_name,
current_sales,
avg(current_sales) over(partition by product_name) as avg_sales,
current_sales - avg(current_sales) over(partition by product_name) as diff_avg,
case when current_sales - avg(current_sales) over(partition by product_name) > 0 then 'Above Average'
	 when current_sales - avg(current_sales) over(partition by product_name) < 0 then  'Below Average'
	 else 'Average'
end as avg_change,
lag(current_sales) over (partition by product_name order by order_year) as prev_year_sales,        --Year-Over_Year Analysis or YOY analysis
current_sales - lag(current_sales) over (partition by product_name order by order_year) as diff_sales,
case when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
	 when current_sales - avg(current_sales) over(partition by product_name) < 0 then  'Decrease'
	 else 'No change'
end as prev_year_change
from year_product_sales
order by product_name, order_year;

/*Analysis of monthly performance of products by comparing 
each products sales to both its average sales performance and the previous month sales*/

--Month-Over-Month Analysis
with monthly_order_sales as (
select 
month(s.order_date) as order_month,
FORMAT(order_date, 'MMMM') as month_name,
p.product_name,
sum(s.sales_amount) as current_sales
from gold.fact_sales s
left join gold.dim_products p
on s.product_key=p.product_key
where order_date is not null
group by month(s.order_date),FORMAT(order_date, 'MMMM'), p.product_name)

select 
month_name,
product_name,
current_sales,
AVG(current_sales) over(partition by product_name) as avg_sales,
current_sales-AVG(current_sales) over(partition by product_name) as diff_avg,
case when current_sales-AVG(current_sales) over(partition by product_name) > 0 then 'Greater than Average'
	 when current_sales-AVG(current_sales) over(partition by product_name) < 0 then 'Lower than Average'
	 else 'Average'
end as avg_change,
lag(current_sales) over(partition by product_name order by order_month) as prev_month_sales,
current_sales-lag(current_sales) over(partition by product_name order by order_month) as diff_sales,
case when current_sales-lag(current_sales) over(partition by product_name order by order_month) >0 then 'Increase'
	 when current_sales-lag(current_sales) over(partition by product_name order by order_month) <0 then 'Decrease'
	 else 'No Change'
end as prev_month_change
from monthly_order_sales
order by product_name;

/*Proportional Analysis - part-to-whole - how an individual part is performing compared to the overall 
                          and helps us understand which category has greater impact in business
  Mathematically - ([measure]/[total measure]) x 100  by dimension or category or country
  like a pie chart*/

-- Which categories contribute most to the overall sales?

with sales_category as (
select 
p.category,
sum(s.sales_amount) as sales
from gold.fact_sales s
left join gold.dim_products p
on s.product_key=p.product_key
group by p.category)

select 
category,
sales,
SUM(sales) over() as total_sales,
concat(round((cast(sales as float)/SUM(sales) over())*100, 2),'%') as perc_contribution
from sales_category
order by sales desc;


/*Data Segmentation - Group the data on a specific range 
					  Helps understand the correlation between 2 measures
  Mathematically - [measure] by [measure]
				   [total customer] by [age]
				   [total products] by [sales range]*/

-- Segment products into cost range and and count how many products fall in each segment

with product_segments as (
select 
product_id,
product_name,
cost,
case when cost<500 then 'Below 100'
	 when cost between 500 and 1000 then '500-1000'
	 when cost between 1000 and 1500 then '1000-1500'
	 when cost between 1500 and 2000 then '1500-2000'
	 else 'Greater 2000'
end cost_range
from gold.dim_products)

select 
cost_range,
COUNT(product_id) as num_products
from product_segments
group by cost_range
order by COUNT(product_id) desc;


--Group the customer into 3 segments based on their spending behavior
	--VIP     - Customer with atleast 12 months of history and spending more than 5000
	--Regular - Customer with atleast 12 months of history and spending 5000 and less
	--New     - Customer with less than 12 month of history
  --And find total number of customer for each segment

with customer_spending as (
select 
c.customer_key,
MIN(s.order_date) as first_order,
MAX(s.order_date) as last_order,
DATEDIFF(MONTH, MIN(s.order_date), MAX(s.order_date)) as lifespan,
sum(s.sales_amount) as total_spending
from gold.fact_sales s
left join gold. dim_customers c
on c.customer_key=s.customer_key
group by c.customer_key)

select
customer_type,
COUNT(customer_key) as num_customer
from(
		select 
		customer_key,
		case when lifespan>12 and total_spending >5000 then 'VIP'
			 when lifespan>12 and total_spending <=5000 then 'Regular'
			 else 'New'
		end as customer_type
		from customer_spending) t
group by customer_type
order by COUNT(customer_key) desc
