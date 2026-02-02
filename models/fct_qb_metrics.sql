{{ config(materialized='table') }}

/**  **/

WITH base_metrics_agg AS (
    SELECT * 
    FROM {{ ref('rw_qb_advanced_metrics') }}
)

, effic_agg AS (
     SELECT * 
    FROM {{ ref('rw_qb_efficiency_metrics') }}
)

, combine AS (
     SELECT * 
    FROM {{ ref('rw_qb_combine') }}
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
    m.PassAttemptsPerGame,
    m.PassingYards,
    m.CompletionPercentage/100 AS CompletionPercentage,
    m.PassingTouchdowns,
    m.Interceptions,
    m.MoneyThrows,
    m.TouchdownRate/100 AS TouchdownRate,
    m.ProtectionRate/100 AS ProtectionRate,
    m.PressuredCompletionPercentage/100 AS PressuredCompletionPercentage,
    m.AirYards,
    m.AirYardsPerGame,
    m.AirYardsPerAttempt,
    m.DeepBallAttempts,
    m.DeepBallCompletionPercentage/100 AS DeepCompletionPercentage,
    e.TruePasserRating,
    e.ReceiverTargetSeparation,
    e.DroppedPasses,
    e.AccuracyRating,
    e.RedZoneAttempts,
    e.RedZoneCompletionPercentage/100 AS RedZoneCompletionPercentage,
    e.InterceptablePasses,
    e.InterceptablePassesPerGame,
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
    a.Position = 'QB'