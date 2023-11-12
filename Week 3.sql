WITH source AS (
    SELECT 
        event_id 
        , session_id 
        , user_id 
        , event_timestamp 
        , PARSE_JSON(event_details) AS event_details       
    FROM vk_data.events.website_activity
)

, extract_data_from_event_details AS (
    SELECT 
        event_id 
        , session_id 
        , user_id 
        , event_timestamp
        , event_details
        , JSON_EXTRACT_PATH_TEXT(event_details, 'event') AS event_name
        , JSON_EXTRACT_PATH_TEXT(event_details, 'page') AS page_name
        , JSON_EXTRACT_PATH_TEXT(event_details, 'recipe_id') AS recipe_id
    FROM source
)

, sessions_found_recipe AS (
    SELECT 
        session_id 
        , recipe_id 
        , event_timestamp
    FROM extract_data_from_event_details
    WHERE recipe_id IS NOT NULL 
)

, combine_event_with_sessions_found_recipe AS (
    SELECT 
        event.event_id 
        , event.session_id 
        , event.user_id 
        , event.event_timestamp
        , event.event_details
        , event.event_name
        , event.page_name
        , event.recipe_id
        , CASE WHEN found_recipe.session_id IS NOT NULL AND event.event_name = 'search' THEN 1 ELSE 0 END AS search_before_recipe
    FROM extract_data_from_event_details AS event
    LEFT JOIN sessions_found_recipe AS found_recipe ON 
        found_recipe.session_id = event.session_id AND 
        event.event_timestamp < found_recipe.event_timestamp   
)

, most_viewed_recipe AS (
    SELECT 
        DATE(event_timestamp) AS date 
        , recipe_id
        , COUNT(event_id) AS count_event
    FROM combine_event_with_sessions_found_recipe
    WHERE recipe_id IS NOT NULL 
    GROUP BY 1,2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY count_event DESC) = 1
)

, session_summary AS (
    SELECT 
        session_id 
        , DATE(event_timestamp) AS date 
        , MIN(event_timestamp) AS session_start_at 
        , MAX(event_timestamp) AS session_end_at
        , SUM(search_before_recipe) AS num_search_before_recipe
    FROM combine_event_with_sessions_found_recipe
    GROUP BY 1,2
)

, add_session_length AS (
    SELECT 
        session_id 
        , date 
        , TIMESTAMPDIFF('second', session_start_at, session_end_at) AS length_in_sec
        , num_search_before_recipe
    FROM session_summary 
)

SELECT 
    ses.date 
    , COUNT(DISTINCT ses.session_id) AS unique_sessions 
    , AVG(ses.length_in_sec) AS avg_session_length_in_sec
    , AVG(ses.num_search_before_recipe) AS avg_num_search_before_recipe
    , rec.recipe_id AS most_viewed_recipe
FROM add_session_length AS ses
LEFT JOIN most_viewed_recipe AS rec ON 
    rec.date = ses.date
GROUP BY ses.date, most_viewed_recipe
