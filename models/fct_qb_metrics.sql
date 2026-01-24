{{ config(materialized='view') }}

/**  **/

WITH base_agg AS (
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