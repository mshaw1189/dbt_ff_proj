{{ config(materialized='view') }}

/**  **/

WITH base_agg AS (
    SELECT 2017 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_qb_metrics_2017

    UNION ALL

    SELECT 2018 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_qb_metrics_2018

    UNION ALL

    SELECT 2019 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_qb_metrics_2019

    UNION ALL

    SELECT 2020 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_qb_metrics_2020

    UNION ALL   

    SELECT 2021 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_qb_metrics_2021

    UNION ALL

    SELECT 2022 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_qb_metrics_2022
)

, effic_agg AS (
    SELECT 2017 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_qb_efficiency_metrics_2017

    UNION ALL

    SELECT 2018 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_qb_efficiency_metrics_2018

    UNION ALL

    SELECT 2019 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_qb_efficiency_metrics_2019

    UNION ALL

    SELECT 2020 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_qb_efficiency_metrics_2020

    UNION ALL

    SELECT 2021 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_qb_efficiency_metrics_2021

    UNION ALL

    SELECT 2022 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_qb_efficiency_metrics_2022    
)

, combine AS (
    SELECT 2011 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2011

    UNION ALL
    SELECT 2012 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2012

    UNION ALL

    SELECT 2013 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2013

    UNION ALL

    SELECT 2014 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2014

    UNION ALL

    SELECT 2015 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2015

    UNION ALL

        SELECT 2016 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2016

    UNION ALL

    SELECT 2017 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2017

    UNION ALL

    SELECT 2018 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2018

    UNION ALL

    SELECT 2019 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2019

    UNION ALL

        SELECT 2020 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2020

    UNION ALL

    SELECT 2021 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2021

    UNION ALL

    SELECT 2022 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2022

    UNION ALL

    SELECT 2023 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_qb_combine_results_2023   
)

SELECT
    a.Year,
    a.Rank,
    a.PlayerPlayerId AS PlayerId,
    a.PlayerShortName,
    a.PlayerAgeExact,
    a.PassAttemptsPerGame,
    a.PassingYards,
    a.CompletionPercentage/100 AS CompletionPercentage,
    a.PassingTouchdowns,
    a.Interceptions,
    a.MoneyThrows,
    a.TouchdownRate/100 AS TouchdownRate,
    a.ProtectionRate/100 AS ProtectionRate,
    a.PressuredCompletionPercentage/100 AS PressuredCompletionPercentage,
    a.AirYards,
    a.AirYardsPerGame,
    a.AirYardsPerAttempt,
    a.DeepBallAttempts,
    a.DeepBallCompletionPercentage/100 AS DeepBallCompletionPercentage,
    b.TruePasserRating,
    b.ReceiverTargetSeparation,
    b.DroppedPasses,
    b.AccuracyRating,
    b.RedZoneAttempts,
    b.RedZoneCompletionPercentage/100 AS RedZoneCompletionPercentage,
    b.InterceptablePasses,
    b.InterceptablePassesPerGame,
    c.PlayerHeight,           -------- Needs adjustment
    CAST(split(c.PlayerHeight, "'")[safe_offset(0)] AS numeric) +
        ROUND(
            CAST(REPLACE(split(c.PlayerHeight, "'")[safe_offset(1)], '"', '') AS numeric)/12,
            2) AS PlayerHeight_Decimal, 
    (CAST(split(c.PlayerHeight, "'")[safe_offset(0)] AS numeric) * 12) +
        CAST(REPLACE(split(c.PlayerHeight, "'")[safe_offset(1)], '"', '') AS numeric) 
        AS PlayerHeight_Inches,
    c.PlayerWeight,
    c.BMI,
    c.HandSize,
    c.FortyYardDash AS cbn_FortyYardDash,
    c.TwentyYardShuttle AS cbn_TwentyYardShuttle,
    c.ThreeConeDrill AS cbn_ThreeConeDrill,
    c.VerticalJump AS cbn_VerticalJump,
    c.BroadJump AS cbn_broadJump,
    c.AgilityScore AS cbn_agilityScore,
    c.AthleticismScore AS cbn_athleticismScore,
    c.BurstScore AS cbn_burstScore,
    c.SPARQx AS cbn_SPARQx
FROM base_agg a
LEFT JOIN effic_agg b 
    ON a.Year = b.Year
       AND a.PlayerPlayerId = b.PlayerPlayerId
LEFT JOIN combine c
    ON a.PlayerPlayerId = c.PlayerPlayerId    