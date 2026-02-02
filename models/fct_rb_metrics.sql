{{ config(materialized='table') }}

WITH base_metrics_agg AS (
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

, snap_counts AS (
    SELECT * 
    FROM {{ ref('rw_snap_counts') }}
)

SELECT
    a.playerid,
    a.name,
    a.year,
    m.PlayerAgeExact,
    a.years_played,
    a.team,
    a.position_rank_total,    
    a.total_fantasy_points,  -- half ppr
    a.fantasy_points_per_game,    
    a.games_played,
    a.SnapsPlayed,
    a.SnapsPerGame,
    a.snap_share,
    a.rush_share,
    a.rush_share_extrap,
    a.target_share,
    a.target_share_extrap, -- extrapolated to account for if played full season
    a.touch_share,		
    a.touch_share_extrap, -- expolated to account for if played full season    
    m.Carries,
    m.RushingYards,
    m.YardsPerCarry,
    m.RushingTouchdowns,
    m.Targets,
    m.Receptions,
    m.ReceivingYards,
    m.ReceivingTDs,
    m.OpportunityShare/100 AS OpportunityShare,
    m.ShotgunCarryRate/100 AS ShotgunCarryRate,
    m.ShotgunYardsPerCarry,
    m.UnderCenterCarryRate/100 AS UnderCenterCarryRate,
    m.UnderCenterYardsPerCarry,
    m.AverageDefendersInTheBox,
    m.StackedFrontCarryRate/100 AS StackedFrontCarryRate,
    m.BaseFrontCarryRate/100 AS BaseFrontCarryRate,
    m.LightFrontCarryRate/100 AS LightFrontCarryRate,
    e.JukeRate/100 AS JukeRate,
    e.EvadedTackles,
    e.YardsCreated,
    e.YardsCreatedPerGame,
    e.YardsCreatedPerCarry,
    e.BreakawayRuns,
    e.BreakawayRunRate/100 AS BreakawayRunRate,
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
FROM {{ ref('fct_players') }} a
LEFT JOIN base_metrics_agg m
    ON a.PlayerID = m.PlayerPlayerId
       AND a.Year = m.Year
LEFT JOIN effic_agg e
    ON a.PlayerID = e.PlayerPlayerId 
        AND m.Year = e.Year
LEFT JOIN combine c
    ON a.PlayerID = c.PlayerPlayerId      
WHERE
    a.Position = 'RB'