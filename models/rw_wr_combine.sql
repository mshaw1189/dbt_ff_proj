{{ config(materialized='view') }}

SELECT 2011 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2011

UNION ALL
SELECT 2012 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2012

UNION ALL

SELECT 2013 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2013

UNION ALL

SELECT 2014 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2014

UNION ALL

SELECT 2015 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2015

UNION ALL

SELECT 2016 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2016

UNION ALL

SELECT 2017 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2017

UNION ALL

SELECT 2018 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2018

UNION ALL

SELECT 2019 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2019

UNION ALL

SELECT 2020 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2020

UNION ALL

SELECT 2021 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2021

UNION ALL

SELECT 2022 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2022

UNION ALL

SELECT 2023 AS Year, * 
FROM ff-dbt.ff_dbt_data_rw.rw_wr_combine_results_2023   