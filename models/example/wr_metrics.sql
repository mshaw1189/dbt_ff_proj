{{ config(materialized='view') }}

WITH base_agg AS (
    SELECT 2017 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_wr_metrics_2017

    UNION ALL

    SELECT 2018 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_wr_metrics_2018

    UNION ALL

    SELECT 2019 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_wr_metrics_2019

    UNION ALL

    SELECT 2020 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_wr_metrics_2020

    UNION ALL

    SELECT 2021 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_wr_metrics_2021

    UNION ALL

    SELECT 2022 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_wr_metrics_2022
)

, effic_agg AS (
    SELECT 2017 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_wr_efficiency_metrics_2017

    UNION ALL

    SELECT 2018 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_wr_efficiency_metrics_2018

    UNION ALL

    SELECT 2019 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_wr_efficiency_metrics_2019

    UNION ALL

    SELECT 2020 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_wr_efficiency_metrics_2020

    UNION ALL

    SELECT 2021 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_wr_efficiency_metrics_2021

    UNION ALL

    SELECT 2022 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_wr_efficiency_metrics_2022 
)

SELECT
    a.Year
FROM base_agg a
LEFT JOIN effic_agg b 
    ON a.Year = b.Year
       AND a.PlayerPlayerId = b.PlayerPlayerId