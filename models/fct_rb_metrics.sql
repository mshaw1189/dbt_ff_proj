{{ config(materialized='view') }}

WITH base_agg AS (
    SELECT * 
    FROM {{ ref('rw_rb_advanced_metrics') }}
)

, effic_agg AS (
     SELECT * 
    FROM {{ ref('rw_rb_efficiency_metrics') }}
)

, combine AS (
     SELECT * 
    FROM {{ ref('rw_rb_combine') }}
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
LEFT JOIN combine c
    ON a.PlayerPlayerId = c.PlayerPlayerId           