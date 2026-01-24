{{ config(materialized='view') }}

WITH base_agg AS (
    SELECT * 
    FROM {{ ref('rw_wr_advanced_metrics') }}
)

, effic_agg AS (
     SELECT * 
    FROM {{ ref('rw_wr_efficiency_metrics') }}
)

SELECT
    a.Year
FROM base_agg a
LEFT JOIN effic_agg b 
    ON a.Year = b.Year
       AND a.PlayerPlayerId = b.PlayerPlayerId