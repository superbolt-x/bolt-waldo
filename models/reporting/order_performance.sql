{{ config (
    alias = target.database + '_order_performance'
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
    FROM {{ source('snowflake_raw_public','orders') }}
    LEFT JOIN {{ source('snowflake_raw_public','ordercustomer') }} oc ON orders.id = oc.orderid
    ),
    
    order_data AS 
    (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY date) as customer_order_index
    FROM 
        (SELECT customer_id, order_id, date, region, customer_acquisition_date, 
            COALESCE(SUM(revenue),0) as order_revenue
        FROM data
        GROUP BY 1,2,3,4,5)
    ),
    
    productlist_data AS
    (SELECT id as order_id, LISTAGG(DISTINCT name, ', ') as product_list
    FROM {{ source('snowflake_raw_public','orders') }}
    LEFT JOIN {{ source('snowflake_raw_public','orderproductvariant') }} opv ON orders.id = opv.orderid
    GROUP BY 1
    )

SELECT date, 
    customer_id, 
    order_id, 
    region, 
    product_list, 
    customer_acquisition_date, 
    customer_order_index, 
    COALESCE(SUM(revenue),0) as total_sales
FROM data
LEFT JOIN order_data USING(customer_id, order_id, date, region, customer_acquisition_date)
LEFT JOIN productlist_data USING(order_id)
GROUP BY 1,2,3,4,5,6,7
