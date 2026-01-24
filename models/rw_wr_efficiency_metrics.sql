{{ config(materialized='view') }}

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