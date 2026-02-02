{{ config(materialized='table') }}

SELECT
    Year,
    PlayerID,
    Name,
    COUNT(year) OVER (PARTITION BY PlayerID) AS years_played,
    Team,
    Position,
    played AS games_played,
    SnapsPlayed,
    SnapsPerGame,
    SnapsPlayedPercentage/100 AS snap_share,
    RushShare/100 AS rush_share, 
    ((RushShare/100) * 17) / played AS rush_share_extrap,   -- extrapolated to account for if played full season (assumes 17 games in season; change to Max per year)
    TargetShare/100 AS target_share,
    ((TargetShare/100) * 17) / played AS target_share_extrap, -- extrapolated to account for if played full season
    TouchSnapPercentage/100 AS touch_share,		
    ((TouchSnapPercentage/100) * 17) / played AS touch_share_extrap, -- expolated to account for if played full season
    FantasyPointsHalfPointPpr AS total_fantasy_points,  -- half ppr
    (FantasyPointsHalfPointPpr/played) AS fantasy_points_per_game,
    DENSE_RANK() OVER(PARTITION BY year, position  ORDER BY total_fantasy_points DESC) AS position_rank_total,
    -- DENSE_RANK() OVER(PARTITION BY position, year ORDER BY (FantasyPointsHalfPointPpr/played) DESC) AS position_rank_per_game, 
    LAG(total_fantasy_points) OVER(PARTITION BY name ORDER BY year) AS points_py
FROM {{ ref('rw_snap_counts') }} 
WHERE 
    Position != 'FB'