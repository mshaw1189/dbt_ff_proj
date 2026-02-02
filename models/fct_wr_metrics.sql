{{ config(materialized='table') }}

WITH base_metrics_agg AS (
    SELECT * 
    FROM {{ ref('rw_wr_advanced_metrics') }}
)

, effic_agg AS (
    SELECT * 
    FROM {{ ref('rw_wr_efficiency_metrics') }}
)

, combine AS (
    SELECT * 
    FROM {{ ref('rw_wr_combine') }}
)

SELECT
    a.playerid,
    a.name,
    a.year,
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
    a.touch_share,		
    a.touch_share_extrap, -- expolated to account for if played full season    
    m.Targets,
    a.target_share,
    a.target_share_extrap, -- extrapolated to account for if played full season    
    m.Receptions,
    m.ReceivingYards,
    m.ReceivingTDs,
    m.RoutesRunPerGame,
    m.AverageTargetDistance,
    m.AirYards,
    m.AirYardsPerGame,
    m.AirYardsPerReception,
    m.RedZoneTargets,
    m.EndzoneTargets,
    m.TargetAccuracy,
    m.TargetShare,
    m.FantasyPointsPerTarget,
    e.Cushion,
    e.TargetSeparation,
    e.TrueCatchRate,
    e.ContestedTargets,
    e.ContestedCatches,
    e.ContestedCatchConversionRate,
    e.YardsPerReception,
    e.YardsPerTarget,
    e.YardsPerPassRoute,
    e.Drops,
    e.DropRate,    
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
    c.CatchRadius AS cbn_CatchRadius,
    c.FortyYardDash AS cbn_FortyYardDash,
    c.TwentyYardShuttle AS cbn_TwentyYardShuttle,
    c.ThreeConeDrill AS cbn_ThreeConeDrill,
    c.VerticalJump AS cbn_VerticalJump,
    c.BroadJump AS cbn_BroadJump,
    c.BenchPress AS cbn_BenchPress,
    c.AgilityScore AS cbn_AgilityScore,
    c.AthleticismScore AS cbn_AthleticismScore,
    c.HeightAdjustedSpeedScore AS cbn_HeightAdjustedSpeedScore,
    c.BurstScore AS cbn_BurstScore,
    c.SPARQx AS cbn_SPARQx,    
FROM {{ ref('fct_players') }} a
LEFT JOIN base_metrics_agg m
    ON a.PlayerID = m.PlayerPlayerId
       AND a.Year = m.Year
LEFT JOIN effic_agg e
    ON a.PlayerID  = e.PlayerPlayerId
        AND m.Year = e.Year
LEFT JOIN combine c
    ON m.PlayerPlayerId = c.PlayerPlayerId         


