{{ config (
    alias = target.database + '_daily_sales_performance'
)}}

{% set date_granularity_list = ['day', 'week', 'month', 'quarter', 'year'] %}

WITH data AS 
    (SELECT
        customerid as customer_id,
        id as order_id,
        createdat::date as date,
        region,
        grandtotalprice as revenue,
        MIN(date) OVER (PARTITION BY customer_id) as customer_acquisition_date
    FROM snowflake_raw_public.orders
    LEFT JOIN snowflake_raw_public.ordercustomer oc ON orders.id = oc.orderid
    ),
    
    daily_data AS 
    (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY date) as customer_order_index
    FROM 
        (SELECT customer_id, order_id, date, region, customer_acquisition_date, 
            COALESCE(SUM(revenue),0) as revenue
        FROM data
        GROUP BY 1,2,3,4,5)
    )

    final_data as
    ({%- for date_granularity in date_granularity_list %}
    SELECT  
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        region,
        COUNT(DISTINCT order_id) as orders, 
        COUNT(DISTINCT CASE WHEN customer_order_index = 1 THEN order_id END) as first_orders,
        COUNT(DISTINCT CASE WHEN customer_order_index > 1 THEN order_id END) as repeat_orders,
        COALESCE(SUM(revenue),0) as total_sales, 
        COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN revenue END),0) as firstorder_totalsales,
        COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN revenue END),0) as repeatorder_totalsales
      FROM daily_data
      GROUP BY 1,2,3
      {% if not loop.last %}UNION ALL
      {% endif %}
    {% endfor %})

SELECT 
date_granularity,
date,
region,
orders,
first_orders,
repeat_orders,
total_sales,
firstorder_totalsales,
repeatorder_totalsales
FROM final_data
