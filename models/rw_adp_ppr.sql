{{ config(materialized='view') }}

SELECT 2016 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_ppr_adp_2016

UNION ALL

SELECT 2017 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_ppr_adp_2017

UNION ALL

SELECT 2018 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_ppr_adp_2018

UNION ALL

SELECT 2019 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_ppr_adp_2019

UNION ALL

SELECT 2020 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_ppr_adp_2020

UNION ALL

SELECT 2021 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_ppr_adp_2021

UNION ALL

SELECT 2022 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_ppr_adp_2022

UNION ALL

SELECT 2023 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_ppr_adp_2022
