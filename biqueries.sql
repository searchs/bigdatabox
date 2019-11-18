-- Test Database: PostGres

SELECT last_name, count(1) AS pops FROM actor
GROUP BY last_name
ORDER BY pops desc;
SELECT * FROM actor LIMIT 2;

-- -- Use EXTRACT function 
SELECT p.payment_id, p.customer_id, p.amount, p.payment_date
FROM payment p
WHERE EXTRACT(month FROM p.payment_date) = 2
AND p.amount > 2
ORDER BY p.payment_id DESC, p.amount ASC;

-- -- Trim date column using ::
SELECT p.payment_date::date, COUNT(*)
FROM payment p
GROUP BY 1 --column 1 of results
ORDER BY 2 DESC;


 --SELECT p.customer_id, SUM(p.amount)
SELECT p.customer_id, ARRAY_AGG(p.payment_date) --get result in array, ready for data scientists
FROM payment p
GROUP BY 1
ORDER BY 2 DESC

-- Generate Sample data
-- LEFT JOIN example
SELECT gs::date, gs2::date 
FROM generate_series('2018-04-01', current_date::date, INTERVAL '1 Day' ) gs
LEFT JOIN generate_series('2018-04-10', current_date::date, INTERVAL '1 Day' ) gs2
ON gs::date = gs2::date

SELECT gs::date, COUNT(*) 
FROM generate_series('2007-02-01', '2007-02-28', INTERVAL '1 Day' ) gs
LEFT JOIN payment p
ON p.payment_date::date = gs::date
GROUP BY 1
HAVING COUNT(*) = 1;


-- Has all inventory even been rented
SELECT f.film_id, f.title,
	i.store_id,i.inventory_id,
	COUNT(distinct r.rental_id) as rentals
FROM film f
	LEFT JOIN inventory i ON i.film_id = f.film_id
	LEFT JOIN rental r ON r.inventory_id = i.inventory_id
GROUP BY 1, 2, 3, 4
ORDER BY 3 NULLS FIRST;

-- Finding a customer's first rental and various attributes about it
-- SELF JOIN
SELECT r.customer_id, MIN(r.rental_id) AS first_order_id, (
SELECT r2.rental_date 
	FROM rental r2
	WHERE r2.rental_id = MIN(r.rental_id)
)::date first_order_date
FROM rental r
GROUP BY 1
ORDER BY 1;

--  How many Customers purchased from multiple stores
SELECT t.customer_id, COUNT(*) FROM
(SELECT DISTINCT r.customer_id, s.store_id
	FROM rental r
	LEFT JOIN staff s ON s.staff_id = r.staff_id 
	ORDER BY 1) t
	GROUP BY 1;
	
	
--Common Table Express (CTE) Examples
WITH base_table AS (
SELECT DISTINCT r.customer_id, s.store_id
	FROM rental r
	LEFT JOIN staff s ON s.staff_id = r.staff_id 
	ORDER BY 1
)

SELECT bt.customer_id, COUNT(*) FROM base_table bt
GROUP BY 1
ORDER BY 1;


-- -- JOIN gotchas, sometimes, if using LEFT JOINS and NULL matters, put the filer on the JOIN itself
SELECT zebra::date, 'zebra', p.*
FROM generate_series('2007-02-01', '2007-02-28', INTERVAL '1 Day' ) zebra
	LEFT JOIN payment p ON p.payment_date::date = zebra::date and p.staff_id = 2
	ORDER BY 3 NULLS FIRST;
	
WITH base_table1 AS (
SELECT zebra::date, 'zebra', p.*
FROM generate_series('2007-02-01', '2007-02-28', INTERVAL '1 Day' ) zebra
	LEFT JOIN payment p ON p.payment_date::date = zebra::date and p.staff_id = 2
	ORDER BY 3 NULLS FIRST
)

-- Chaining multiple conditions where OR is involved
-- from rental_id > 1400 and payment hour is between 8am and noon OR 2pm to 3pm
SELECT * 
FROM base_table1 bt
WHERE bt.rental_id > 1400
AND EXTRACT(HOUR FROM bt.payment_date) IN (8,9,10,11,12,14)
ORDER BY 6; --rental_id is in column 6 bro!


-- WHERE vs HAVING 
-- Return Customers whose first order was on a weekend and worth over 5 and who's spent at least 100 total
-- Note: Sunday =0, Saturday = 6
-- CLV: Customer Lifetime Value

SELECT p.*, EXTRACT(dow FROM p.payment_date) AS day_week,

(
SELECT SUM(p3.amount)
FROM payment p3
	WHERE p3.customer_id = p.customer_id
) as CLV

FROM payment p
WHERE p.payment_id = (
	SELECT MIN(p2.payment_id)
	FROM payment p2
	WHERE p2.customer_id = p.customer_id
)
AND EXTRACT(dow FROM p.payment_date) IN (0,6)
AND p.amount > 5
GROUP BY 1
HAVING
(
SELECT SUM(p3.amount)
FROM payment p3
	WHERE p3.customer_id = p.customer_id
) > 100;


-- BigQuery Analytics
-- Qwiklabs Sessions
-- Find Duplicate values
-- NUGGETS:
-- In your own datasets, even if you have a unique key, it is still beneficial to 
-- confirm the uniqueness of the rows with COUNT, GROUP BY, and HAVING before you 
-- begin your analysis.

#standardSQL
SELECT COUNT(*) as num_duplicate_rows, * FROM
`data-to-insights.ecommerce.all_sessions_raw`
GROUP BY fullVisitorId,
            channelGrouping,
            time, 
            country,
            city,
            totalTransactionRevenue,
            transactions,
            timeOnSite,
            pageviews, 
            sessionQualityDim,
            date,
            visitId,
            type,
            productRefundAmount,
            productQuantity, 
            productPrice, productRevenue, productSKU, v2ProductName, v2ProductCategory, 
productVariant, currencyCode, itemQuantity, itemRevenue, transactionRevenue, 
transactionId, pageTitle, searchKeyword, pagePathLevel1, eCommerceAction_type, 
eCommerceAction_step, eCommerceAction_option
HAVING num_duplicate_rows > 1;


-- Confirm that no duplicates exist
#standardSQL
# schema: https://support.google.com/analytics/answer/3437719?hl=en
SELECT
fullVisitorId, # the unique visitor ID
visitId, # a visitor can have multiple visits
date, # session date stored as string YYYYMMDD
time, # time of the individual site hit  (can be 0 to many per visitor session)
v2ProductName, # not unique since a product can have variants like Color
productSKU, # unique for each product
type, # a visitor can visit Pages and/or can trigger Events (even at the same time)
eCommerceAction_type, # maps to ‘add to cart', ‘completed checkout'
eCommerceAction_step,
eCommerceAction_option,
  transactionRevenue, # revenue of the order
  transactionId, # unique identifier for revenue bearing transaction
COUNT(*) as row_count
FROM
`data-to-insights.ecommerce.all_sessions`
GROUP BY 1,2,3 ,4, 5, 6, 7, 8, 9, 10,11,12
HAVING row_count > 1 # find duplicates

-- Note: In SQL, you can GROUP BY or ORDER BY the index of the column like using "GROUP BY 1" instead of "GROUP BY fullVisitorId"



-- determines the total views by counting product_views and the number of unique visitors by counting fullVisitorID
#standardSQL
SELECT
  COUNT(*) AS product_views,
  COUNT(DISTINCT fullVisitorId) AS unique_visitors
FROM `data-to-insights.ecommerce.all_sessions`;

-- Now write a query that shows total unique visitors(fullVisitorID) by the referring site (channelGrouping):
#standardSQL
SELECT
  COUNT(DISTINCT fullVisitorId) AS unique_visitors,
  channelGrouping
FROM `data-to-insights.ecommerce.all_sessions`
GROUP BY channelGrouping
ORDER BY channelGrouping DESC;

-- list all the unique product names (v2ProductName) alphabetically:
#standardSQL
SELECT
  (v2ProductName) AS ProductName
FROM `data-to-insights.ecommerce.all_sessions`
GROUP BY ProductName
ORDER BY ProductName


-- list the five products with the most views (product_views) from all visitors (include people who have viewed the same product more than once)
#standardSQL
SELECT
  COUNT(*) AS product_views,
  (v2ProductName) AS ProductName
FROM `data-to-insights.ecommerce.all_sessions`
WHERE type = 'PAGE'
GROUP BY v2ProductName
ORDER BY product_views DESC
LIMIT 5;


-- Now refine the query to no longer double-count product views for visitors who have viewed a product many times. Each distinct product view should only count once per visitor.
WITH unique_product_views_by_person AS (
-- find each unique product viewed by each visitor
SELECT 
 fullVisitorId,
 (v2ProductName) AS ProductName
FROM `data-to-insights.ecommerce.all_sessions`
WHERE type = 'PAGE'
GROUP BY fullVisitorId, v2ProductName )


-- aggregate the top viewed products and sort them
SELECT
  COUNT(*) AS unique_view_count,
  ProductName 
FROM unique_product_views_by_person
GROUP BY ProductName
ORDER BY unique_view_count DESC
LIMIT 5


-- expand your previous query to include the total number of distinct products ordered and the total number of total units ordered (productQuantity)
#standardSQL
SELECT
  COUNT(*) AS product_views,
  COUNT(productQuantity) AS orders,
  SUM(productQuantity) AS quantity_product_ordered,
  v2ProductName
FROM `data-to-insights.ecommerce.all_sessions`
WHERE type = 'PAGE'
GROUP BY v2ProductName
ORDER BY product_views DESC
LIMIT 5;


#standardSQL
SELECT
  COUNT(*) AS product_views,
  COUNT(productQuantity) AS orders,
  SUM(productQuantity) AS quantity_product_ordered,
  SUM(productQuantity) / COUNT(productQuantity) AS avg_per_order,
  (v2ProductName) AS ProductName
FROM `data-to-insights.ecommerce.all_sessions`
WHERE type = 'PAGE'
GROUP BY v2ProductName
ORDER BY product_views DESC
LIMIT 5;


--Expand the query to include the average amount of product per order (total number of units ordered/total number of orders, or SUM(productQuantity)/COUNT(productQuantity)).

#standardSQL
SELECT
  COUNT(*) AS product_views,
  COUNT(productQuantity) AS orders,
  SUM(productQuantity) AS quantity_product_ordered,
  SUM(productQuantity) / COUNT(productQuantity) AS avg_per_order,
  (v2ProductName) AS ProductName
FROM `data-to-insights.ecommerce.all_sessions`
WHERE type = 'PAGE'
GROUP BY v2ProductName
ORDER BY product_views DESC
LIMIT 5;



