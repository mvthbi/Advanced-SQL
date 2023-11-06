WITH us_cities AS (
    SELECT 
        LOWER(TRIM(city_name)) AS city_name
        , UPPER(TRIM(state_abbr)) AS state_abbr
        , geo_location
    FROM vk_data.resources.us_cities
)

, get_chicago_loc AS (
    SELECT 
        geo_location
    FROM us_cities
    WHERE city_name = 'chicago' AND state_abbr = 'IL'
)

, get_gary_loc AS (
    SELECT 
        geo_location
    FROM us_cities
    WHERE city_name = 'gary' AND state_abbr = 'IN'
)

, impacted_customer AS (
    SELECT 
        c.customer_id
        , c.first_name || ' ' || c.last_name AS customer_name
        , TRIM(ca.customer_city) AS customer_city
        , ca.customer_state
        , us.geo_location
    FROM vk_data.customers.customer_address AS ca
    JOIN vk_data.customers.customer_data AS c ON ca.customer_id = c.customer_id
    LEFT JOIN us_cities AS us 
        ON UPPER(TRIM(ca.customer_state)) = us.state_abbr
        AND TRIM(LOWER(ca.customer_city)) = us.city_name
    WHERE (
        us.city_name IN ('concord','georgetown','ashland') AND ca.customer_state = 'KY'
    ) OR (
        us.city_name IN ('oakland','pleasant hill') AND ca.customer_state = 'CA' 
    ) OR (
        us.city_name IN ('arlington','brownsville') AND ca.customer_state = 'TX' 
    )
)

, get_distance AS (
    SELECT 
        ic.customer_id
        , ic.customer_name
        , ic.customer_city
        , ic.customer_state
        , ic.geo_location
        , (ST_DISTANCE(ic.geo_location, chic.geo_location) / 1609)::INT AS chicago_distance_miles
        , (ST_DISTANCE(ic.geo_location, gary.geo_location) / 1609)::INT AS gary_distance_miles
    FROM impacted_customer AS ic
    CROSS JOIN get_chicago_loc AS chic 
    CROSS JOIN get_gary_loc AS gary
)

, food_pref AS (
    SELECT 
        customer_id
        , COUNT(*) AS food_pref_count
    FROM vk_data.customers.customer_survey
    WHERE 
        is_active
    GROUP BY 1
)

SELECT 
    gd.customer_name
    , gd.customer_city
    , gd.customer_state
    , fp.food_pref_count
    , gd.chicago_distance_miles
    , gd.gary_distance_miles
FROM get_distance AS gd
JOIN food_pref AS fp
    ON fp.customer_id = gd.customer_id
    
