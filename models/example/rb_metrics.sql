{{ config(materialized='view') }}

WITH base_agg AS (
    SELECT 2017 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_rb_metrics_2017

    UNION ALL

    SELECT 2018 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_rb_metrics_2018

    UNION ALL

    SELECT 2019 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_rb_metrics_2019

    UNION ALL

    SELECT 2020 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_rb_metrics_2020

    UNION ALL

    SELECT 2021 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_rb_metrics_2021

    UNION ALL

    SELECT 2022 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_rb_metrics_2022
)

, effic_agg AS (
    SELECT 2017 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_rb_efficiency_metrics_2017

    UNION ALL

    SELECT 2018 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_rb_efficiency_metrics_2018

    UNION ALL

    SELECT 2019 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_rb_efficiency_metrics_2019

    UNION ALL

    SELECT 2020 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_rb_efficiency_metrics_2020

    UNION ALL

    SELECT 2021 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_rb_efficiency_metrics_2021

    UNION ALL

    SELECT 2022 AS Year, * 
    FROM ff-dbt.ff_dbt_data_rw.rw_advanced_rb_efficiency_metrics_2022 
)

SELECT
    a.Year,
    a.Rank,
    a.PlayerPlayerId,
    a.PlayerShortName,
    a.PlayerAgeExact,
    a.Carries,
    a.RushingYards,
    a.YardsPerCarry,
    a.RushingTouchdowns,
    a.Targets,
    a.Receptions,
    a.ReceivingYards,
    a.ReceivingTDs,
    a.SnapShare/100 AS SnapShare,
    a.OpportunityShare/100 AS OpportunityShare,
    a.ShotgunCarryRate/100 AS ShotgunCarryRate,
    a.ShotgunYardsPerCarry,
    a.UnderCenterCarryRate/100 AS UnderCenterCarryRate,
    a.UnderCenterYardsPerCarry,
    a.AverageDefendersInTheBox,
    a.StackedFrontCarryRate/100 AS StackedFrontCarryRate,
    a.BaseFrontCarryRate/100 AS BaseFrontCarryRate,
    a.LightFrontCarryRate/100 AS LightFrontCarryRate,
    b.JukeRate/100 AS JukeRate,
    b.EvadedTackles,
    b.YardsCreated,
    b.YardsCreatedPerGame,
    b.YardsCreatedPerCarry,
    b.BreakawayRuns,
    b.BreakawayRunRate/100 AS BreakawayRunRate        
FROM base_agg a
LEFT JOIN effic_agg b 
    ON a.Year = b.Year
       AND a.PlayerPlayerId = b.PlayerPlayerId