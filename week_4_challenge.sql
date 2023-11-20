/*PART 1
SQL Challenge*/
WITH urgent_orders AS (
    SELECT 
        o.o_orderkey AS order_key
        , o.o_custkey AS cust_key
        , o.o_orderstatus AS order_status
        , o.o_totalprice AS total_price
        , o.o_orderdate AS order_date
        , p.p_partkey AS part_key
        , l.l_quantity AS quantity
        , l.l_extendedprice AS extended_price
        , ROW_NUMBER() OVER (PARTITION BY o.o_custkey ORDER BY o.o_totalprice DESC) AS rank_order
        , ROW_NUMBER() OVER (PARTITION BY o.o_custkey ORDER BY l.l_extendedprice DESC) AS rank_part
    FROM orders AS o
    INNER JOIN snowflake_sample_data.tpch_sf1.customer AS c ON 
        o.o_custkey = c.c_custkey
    INNER JOIN snowflake_sample_data.tpch_sf1.lineitem AS l ON 
        l.l_orderkey = o.o_orderkey
    INNER JOIN snowflake_sample_data.tpch_sf1.part AS p ON 
        p.p_partkey = l.l_partkey
    WHERE o.o_orderpriority = '1-URGENT'
        AND c.c_mktsegment = 'AUTOMOBILE'
)

, top_orders AS (
    SELECT 
        cust_key 
        , LISTAGG(DISTINCT order_key, ', ') AS order_numbers 
        , SUM(total_price) AS total_spent
    FROM urgent_orders
    WHERE rank_order < 4
    GROUP BY 1
)

, latest_order AS (
    SELECT 
        cust_key
        , MAX(order_date) AS last_order_date
    FROM urgent_orders
    GROUP BY 1
)

SELECT 
    lo.cust_key
    , lo.last_order_date
    , top_orders.order_numbers
    , top_orders.total_spent
    , u1.part_key AS part_1_key
    , u1.quantity AS part_1_quantity
    , u1.extended_price AS part_1_total_spent
    , u2.part_key AS part_2_key
    , u2.quantity AS part_2_quantity
    , u2.extended_price AS part_2_total_spent
    , u3.part_key AS part_3_key
    , u3.quantity AS part_3_quantity
    , u3.extended_price AS part_3_total_spent
FROM latest_order AS lo
JOIN top_orders ON top_orders.cust_key = lo.cust_key 
JOIN urgent_orders AS u1 ON 
    lo.cust_key = u1.cust_key AND
    u1.rank_part = 1
LEFT JOIN urgent_orders AS u2 ON 
    u2.cust_key = lo.cust_key AND 
    u2.rank_part = 2
LEFT JOIN urgent_orders AS u3 ON 
    u3.cust_key = lo.cust_key AND 
    u3.rank_part = 3
ORDER BY last_order_date DESC
LIMIT 100


/*PART 2
Candidate's Submission*/

/*
--- Do you agree with the results returned by the query?
No, I think there's a mistake in candidate's submission. They already filtered the top 3 highest price per part before
getting the latest order date, and the top 3 order total. This will make the result inaccurate.

And they do inner join to top 2 and top 3 highest rank. There are customers who only order 1 or 2 products and they 
won't be included because of the inner join.

--- Is it easy to understand?
Yes 
*/
