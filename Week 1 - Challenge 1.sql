/*Approach: 
1. Get customer's geo location by joining customer tables with us_cities table 
2. Filter out customers whose cities and state don't present in the database
3. Get supplier's geo location by joining supplier table with us_cities table 
4. Cross join eligible customer and supplier cte, and calculate the distance between customer to each supplier.
5. Use row_number function to rank supplier by distance by each customer.
6. Finally, only select the rows which is ranked first to get the closest supplier. Order the final result by last_name and first_name.
*/
WITH customer_and_address AS (
    SELECT 
        customer.customer_id 
        , customer.first_name 
        , customer.last_name 
        , customer.email 
        , address.customer_city
        , address.customer_state
        , us_cities.geo_location AS customer_geo_location
    FROM customers.customer_data AS customer
    LEFT JOIN customers.customer_address AS address 
        ON address.customer_id = customer.customer_id
    LEFT JOIN resources.us_cities AS us_cities 
        ON LOWER(TRIM(us_cities.city_name)) = LOWER(TRIM(address.customer_city))
        AND LOWER(TRIM(us_cities.state_abbr)) = LOWER(TRIM(address.customer_state))
)
, eligible_customer AS (
    SELECT 
        *
    FROM customer_and_address
    WHERE customer_geo_location IS NOT NULL
)
, supplier_list AS (
    SELECT 
        supplier_id 
        , supplier_name 
        , supplier_city 
        , supplier_state
        , us_cities.geo_location AS supplier_geo_location
    FROM suppliers.supplier_info
    LEFT JOIN resources.us_cities AS us_cities 
        ON LOWER(us_cities.city_name) = LOWER(supplier_info.supplier_city)
        AND us_cities.state_abbr = supplier_info.supplier_state
)
, customer_and_supplier_distance AS (
    SELECT 
        *,
        ST_DISTANCE(customer_geo_location,supplier_geo_location) / 1000 AS distance_km
    FROM eligible_customer 
    JOIN supplier_list 
),
rank_supplier AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY distance_km) AS rank_number
    FROM customer_and_supplier_distance
)
SELECT 
    customer_id 
    , first_name 
    , last_name 
    , email
    , supplier_id 
    , supplier_name
    , distance_km
FROM rank_supplier
WHERE rank_number = 1
ORDER BY last_name, first_name
