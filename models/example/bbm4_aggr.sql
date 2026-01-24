{{ config(materialized='table') }}

WITH rnd1 AS (
    SELECT
        tournament_round_number,
        draft_id,
        DATE(draft_created_time) AS draft_created_date,
        DATE(draft_completed_time) AS draft_completed_date,
        CASE WHEN clock = 30 THEN 'fast' ELSE 'slow' END AS draft_clock,
        tournament_entry_id AS drafted_team_id,
        username,
        pick_order AS starting_draft_position,
        team_pick_number,
        overall_pick_number,
        projection_adp,
        ROUND(overall_pick_number - projection_adp, 2) AS adp_vs_projected,  -- positive means drafted later than projected
        player_id,
        player_name,
        position_name AS position,
        COUNT(player_name) OVER(PARTITION BY draft_id, position_name 
                                ORDER BY overall_pick_number) AS drafted_position_rank,   -- e.g. WR3 drafted overall (in that draft)
        source AS pick_source,                                               -- auto or manual
        pick_points AS pick_points_rnd1,
        roster_points AS roster_points_rnd1,
        ROUND(pick_points/roster_points,2) AS pct_roster_round_points_rnd1,
        SUM(pick_points) OVER(PARTITION BY draft_id, tournament_entry_id
                                ORDER BY team_pick_number
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pick_points_rnd1
        --     -- players drafter per position
        --     -- points through each round (sum, pct)
        --     -- points per position
    FROM ff-dbt.ff_dbt_data_rw.rw_bbm_iv_2023_r1_results
)

SELECT
    r1.*,
    CASE WHEN r2.tournament_entry_id IS NOT NULL THEN 1 ELSE 0
         END AS advanced_flag_rnd2,
    r2.pick_points AS pick_points_rnd2,
    r2.roster_points AS roster_points_rnd2,
    r2.pick_points / r2.roster_points AS pct_roster_round_points_rnd2,
    CASE WHEN r3.tournament_entry_id IS NOT NULL THEN 1 ELSE 0
         END AS advanced_flag_rnd3,
    r3.pick_points AS pick_points_rnd3,
    r3.roster_points AS roster_points_rnd3,
    r3.pick_points / r3.roster_points AS pct_roster_round_points_rnd3,
    CASE WHEN r4.tournament_entry_id IS NOT NULL THEN 1 ELSE 0
         END AS advanced_flag_rnd4,
    r4.pick_points AS pick_points_rnd4,
    r4.roster_points AS roster_points_rnd4,
    r4.pick_points / r4.roster_points AS pct_roster_round_points_rnd4,
    DENSE_RANK() OVER(ORDER BY r4.roster_points DESC) AS final_rank
FROM rnd1 r1
LEFT JOIN ff-dbt.ff_dbt_data_rw.rw_bbm_iv_2023_r2_results r2 
    ON r1.drafted_team_id = r2.tournament_entry_id
       AND r1.player_id = r2.player_id 
LEFT JOIN ff-dbt.ff_dbt_data_rw.rw_bbm_iv_2023_r3_results r3
    ON r1.drafted_team_id = r3.tournament_entry_id
       AND r1.player_id = r3.player_id
LEFT JOIN ff-dbt.ff_dbt_data_rw.rw_bbm_iv_2023_r4_results r4
    ON r1.drafted_team_id = r4.tournament_entry_id
       AND r1.player_id = r4.player_id        
ORDER BY r1.draft_created_date, r1.draft_id, r1.starting_draft_position, r1.team_pick_number        