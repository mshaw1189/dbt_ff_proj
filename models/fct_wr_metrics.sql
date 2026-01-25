{{ config(materialized='view') }}

WITH base_agg AS (
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
    a.Year,
    a.PlayerPlayerId As PlayerId,
    a.PlayerShortName,
    a.Targets,
    a.Receptions,
    a.ReceivingYards,
    a.ReceivingTDs,
    a.RoutesRunPerGame,
    a.SnapShare,
    a.AverageTargetDistance,
    a.AirYards,
    a.AirYardsPerGame,
    a.AirYardsPerReception,
    a.RedZoneTargets,
    a.EndzoneTargets,
    a.TargetAccuracy,
    a.TargetShare,
    a.FantasyPointsPerTarget,
    b.Cushion,
    b.TargetSeparation,
    b.TrueCatchRate,
    b.ContestedTargets,
    b.ContestedCatches,
    b.ContestedCatchConversionRate,
    b.YardsPerReception,
    b.YardsPerTarget,
    b.YardsPerPassRoute,
    b.Drops,
    b.DropRate,    
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
FROM base_agg a
LEFT JOIN effic_agg b 
    ON a.Year = b.Year
       AND a.PlayerPlayerId = b.PlayerPlayerId
LEFT JOIN combine c
    ON a.PlayerPlayerId = c.PlayerPlayerId         