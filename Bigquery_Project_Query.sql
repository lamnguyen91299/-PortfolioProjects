-- Big project for SQL

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL

SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
    SUM(totals.visits) as visits,
    SUM(totals.pageviews) as pageviews,
    SUM(totals.transactions) as transactions,
    SUM(totals.totalTransactionRevenue) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as data
WHERE _table_suffix between '20170101' and '20170331'
GROUP BY month
ORDER BY 1
-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL

SELECT
    trafficSource.source,
    SUM(totals.visits) as total_visits,
    SUM(totals.bounces) as total_no_of_bounces,
    SUM(totals.bounces)/SUM(totals.visits)*100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as data
Where _table_suffix between '20170701' and '20170731'
GROUP BY trafficSource.source
ORDER BY 2 DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017


SELECT
    'WEEK'as time_type,
    FORMAT_DATE("%Y%W",PARSE_DATE('%Y%m%d',date)) as time,
    trafficSource.source as source,
    SUM(totals.totalTransactionRevenue) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as data
Where _table_suffix between '20170601' and '20170630'
GROUP BY time, source 
UNION ALL
SELECT
    'MONTH'as time_type,
    FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as time,
    trafficSource.source as source,
    SUM(totals.totalTransactionRevenue) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as data
Where _table_suffix between '20170601' and '20170630'
GROUP BY time, source  
ORDER BY 4 DESC



--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. 
Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH nonpurchase as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_nonpurchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as data
    Where 
        _table_suffix between '20170601' and '20170731'
        AND totals.transactions IS NULL
    GROUP BY month
)
, purchase as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        sum(totals.pageviews)/count(distinct fullVisitorId) as avg_pageviews_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as data
    Where 
        _table_suffix between '20170601' and '20170731'
        AND totals.transactions >= 1
    GROUP BY month
)

SELECT
    nonpurchase.month,
    purchase.avg_pageviews_purchase,
    nonpurchase.avg_pageviews_nonpurchase
FROM nonpurchase
JOIN purchase USING(month)


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
    sum(totals.transactions)/count(distinct fullVisitorId)
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as data
Where 
    _table_suffix between '20170701' and '20170731'
    AND totals.transactions >= 1
GROUP BY month



-- Query 06: Average amount of money spent per session
#standardSQL
SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
    sum(totals.totalTransactionRevenue)/count(totals.visits) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as data
Where 
    _table_suffix between '20170701' and '20170731'
    AND totals.transactions >= 1
GROUP BY month



-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
WITH product as (
    SELECT
        fullVisitorId,
        product.v2ProductName,
        product.productRevenue,
        product.productQuantity 
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as data,
        UNNEST(hits) as hits,
        UNNEST(hits.product) as product
    Where 
        _table_suffix between '20170701' and '20170731'
        AND product.productRevenue IS NOT NULL
)

SELECT
    product.v2ProductName as other_purchased_products,
    SUM(product.productQuantity) as quantity
FROM product
WHERE 
    product.fullVisitorId IN (
        SELECT fullVisitorId
        FROM product
        WHERE product.v2ProductName LIKE "YouTube Men's Vintage Henley"

    )
    AND product.v2ProductName NOT LIKE "YouTube Men's Vintage Henley"
GROUP BY other_purchased_products
ORDER BY 2 DESC



--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
WITH product_view as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        COUNT(product.productSKU) as num_product_view
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as data,
        UNNEST(hits) as hits,
        UNNEST(product) as product
    WHERE 
        _table_suffix between '20170101' and '20170331'
        AND eCommerceAction.action_type = '2'
    GROUP BY month
)

, addtocart as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        COUNT(product.productSKU) as num_addtocart
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as data,
        UNNEST(hits) as hits,
        UNNEST(product) as product
    WHERE 
        _table_suffix between '20170101' and '20170331'
        AND eCommerceAction.action_type = '3'
    GROUP BY month
)

, purchase as (
    SELECT
        FORMAT_DATE("%Y%m",PARSE_DATE('%Y%m%d',date)) as month,
        COUNT(product.productSKU) as num_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` as data,
        UNNEST(hits) as hits,
        UNNEST(product) as product
    WHERE 
        _table_suffix between '20170101' and '20170331'
        AND eCommerceAction.action_type = '6'
    GROUP BY month
)

SELECT
    product_view.month,
    product_view.num_product_view,
    addtocart.num_addtocart,
    purchase.num_purchase,
    ROUND((num_addtocart/num_product_view)*100.0,2) as add_to_cart_rate,
    ROUND((num_purchase/num_product_view)*100,2) as purchase_rate
FROM product_view
JOIN addtocart USING(month)
JOIN purchase USING(month)
ORDER BY 1

