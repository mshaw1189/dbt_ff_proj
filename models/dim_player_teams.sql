SELECT
    a.Year,
    PlayerID,
    a.Name,
    Team,
    Position
FROM {{ ref('rw_snap_counts') }} a