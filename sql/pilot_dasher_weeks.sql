WITH pilot_dashers AS (
    SELECT *
    FROM (VALUES
        (37091042, 'Cynthia',        300.00, 0.20, 21),
        (17709935, 'Rudy',           404.00, 0.10, 20),
        (58777515, 'Jonathan N',     363.00, 0.10, 14),
        (14042159, 'Christy',        437.00, 0.15, 8),
        (19473824, 'Efren',         6000.00, 0.05, 8),
        (70682470, 'Marc Compher',  1711.00, 0.15, 9),
        (59877246, 'John Baker',    3700.00, 0.10, 32),
        (23609809, 'Kimberly',      2529.00, 0.10, 9),
        (70847568, 'Dylan',          253.00, 0.05, 11),
        (67778641, 'Angelo',         825.00, 0.20, 11),
        (23449826, 'Crystal Shead',  484.00, 0.05, 25),
        (46601725, 'Preston',       2000.00, 0.10, 18)
    ) AS t(
        dasher_id,
        participant_name,
        card_balance,
        planned_sj_allocation_pct,
        recommended_dashing_hours
    )
),

pilot_weeks AS (
    SELECT
        DATEADD('week', seq, '2026-03-23'::DATE) AS week_start,
        DATEADD('day', 6, DATEADD('week', seq, '2026-03-23'::DATE)) AS week_end
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS seq
        FROM TABLE(GENERATOR(ROWCOUNT => 26))
    )
),

dasher_weeks AS (
    SELECT
        pd.*,
        pw.week_start,
        pw.week_end
    FROM pilot_dashers pd
    CROSS JOIN pilot_weeks pw
),

dashing AS (
    SELECT
        ds.dasher_id,
        DATE_TRUNC('week', ds.active_date) AS week_start,
        SUM(ds.adj_shift_seconds) / 3600.0 AS hours_dashed,
        SUM(ds.total_pay_usd) / 100.0 AS dash_earnings
    FROM edw.dasher.dasher_shifts ds
    INNER JOIN pilot_dashers pd
        ON ds.dasher_id = pd.dasher_id
    WHERE ds.active_date >= '2026-03-23'
    GROUP BY 1, 2
),

provider_to_dasher AS (
    SELECT
        pr.id AS provider_account_id,
        pa.id AS payout_account_id,
        dd.dasher_id
    FROM payout_service.public.provider_accounts pr
    INNER JOIN payout_service.public.payout_accounts pa
        ON pr.payout_account_id = pa.id
    INNER JOIN proddb.public.dimension_dasher dd
        ON dd.payment_account_id = pa.payment_account_id
    WHERE dd.dasher_id IS NOT NULL
),

jar_movements AS (
    SELECT
        p2d.dasher_id,
        DATE_TRUNC('week', t.created_at) AS week_start,
        SUM(CASE WHEN t.transfer_type = 'ManualSave' THEN t.amount / 100.0 ELSE 0 END) AS manual_save_deposits,
        SUM(CASE WHEN t.transfer_type = 'AutoSave' THEN t.amount / 100.0 ELSE 0 END) AS auto_save_deposits,
        SUM(CASE WHEN t.transfer_type = 'ManualWithdraw' THEN t.amount / 100.0 ELSE 0 END) AS jar_outflow
    FROM payout_service.public.money_movement_transfers t
    INNER JOIN provider_to_dasher p2d
        ON t.provider_account_id = p2d.provider_account_id
    INNER JOIN pilot_dashers pd
        ON p2d.dasher_id = pd.dasher_id
    WHERE t.transfer_type IN ('ManualSave', 'AutoSave', 'ManualWithdraw')
      AND t.created_at >= '2026-03-23'
    GROUP BY 1, 2
),

balance_events AS (
    SELECT
        p2d.dasher_id,
        t.created_at,
        DATE_TRUNC('week', t.created_at) AS week_start,
        TRY_PARSE_JSON(t.metadata):ending_balance_in_cents::FLOAT / 100.0 AS end_balance
    FROM payout_service.public.money_movement_transfers t
    INNER JOIN provider_to_dasher p2d
        ON t.provider_account_id = p2d.provider_account_id
    INNER JOIN pilot_dashers pd
        ON p2d.dasher_id = pd.dasher_id
    WHERE t.transfer_type IN ('ManualSave', 'AutoSave', 'ManualWithdraw')
      AND t.created_at >= '2026-03-23'
      AND TRY_PARSE_JSON(t.metadata):ending_balance_in_cents IS NOT NULL
),

weekly_end_balance AS (
    SELECT
        dasher_id,
        week_start,
        end_balance
    FROM balance_events
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dasher_id, week_start
        ORDER BY created_at DESC
    ) = 1
),

allocation_events AS (
    SELECT DISTINCT
        TRY_TO_NUMBER(iguazu_user_id) AS dasher_id,
        iguazu_timestamp,
        TRY_TO_NUMBER(
            TRY_PARSE_JSON(iguazu_other_properties):selected_allocation_percentage::TEXT
        ) AS allocation_pct
    FROM iguazu.driver.m_dxdr_card_action_success
    WHERE provider = 'DXDR_FISERV'
      AND action = 'UPDATE_SAVINGS_ALLOCATION'
      AND page = 'SAVINGS_JAR_CHANGE_PAYOUT'
      AND iguazu_user_id IS NOT NULL
      AND iguazu_timestamp >= '2026-01-01'

    UNION ALL

    SELECT DISTINCT
        TRY_TO_NUMBER(iguazu_user_id) AS dasher_id,
        iguazu_timestamp,
        TRY_TO_NUMBER(
            TRY_PARSE_JSON(iguazu_other_properties):selected_allocation_percentage::TEXT
        ) AS allocation_pct
    FROM iguazu.driver.m_dxdr_card_action_success
    WHERE page = 'SAVINGS_JAR_TURN_OFF_AUTOMATIC_PAYOUT_SHEET'
      AND iguazu_user_id IS NOT NULL
      AND iguazu_timestamp >= '2026-01-01'
),

allocation_start AS (
    SELECT
        dw.dasher_id,
        dw.week_start,
        ae.allocation_pct AS allocation_start_of_wk
    FROM dasher_weeks dw
    LEFT JOIN allocation_events ae
        ON dw.dasher_id = ae.dasher_id
       AND ae.iguazu_timestamp < dw.week_start
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dw.dasher_id, dw.week_start
        ORDER BY ae.iguazu_timestamp DESC
    ) = 1
),

allocation_end AS (
    SELECT
        dw.dasher_id,
        dw.week_start,
        ae.allocation_pct AS allocation_end_of_wk
    FROM dasher_weeks dw
    LEFT JOIN allocation_events ae
        ON dw.dasher_id = ae.dasher_id
       AND ae.iguazu_timestamp < DATEADD('day', 7, dw.week_start)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dw.dasher_id, dw.week_start
        ORDER BY ae.iguazu_timestamp DESC
    ) = 1
),

allocation_within_week AS (
    SELECT
        ae.dasher_id,
        DATE_TRUNC('week', ae.iguazu_timestamp) AS week_start,
        MIN(ae.allocation_pct) AS allocation_min_in_wk,
        MAX(ae.allocation_pct) AS allocation_max_in_wk
    FROM allocation_events ae
    WHERE ae.iguazu_timestamp >= '2026-03-23'
    GROUP BY 1, 2
)

SELECT
    dw.dasher_id,
    dw.participant_name,
    dw.card_balance,
    dw.planned_sj_allocation_pct,
    dw.recommended_dashing_hours,

    dw.week_start,
    dw.week_end,

    COALESCE(da.hours_dashed, 0) AS hours_dashed,
    COALESCE(da.dash_earnings, 0) AS dash_earnings,

    COALESCE(jm.manual_save_deposits, 0) AS manual_save_deposits,
    COALESCE(jm.auto_save_deposits, 0) AS auto_save_deposits,
    COALESCE(jm.jar_outflow, 0) AS jar_outflow,

    web.end_balance,

    ast.allocation_start_of_wk,
    aen.allocation_end_of_wk,
    aww.allocation_min_in_wk,
    aww.allocation_max_in_wk

FROM dasher_weeks dw
LEFT JOIN dashing da
    ON dw.dasher_id = da.dasher_id
   AND dw.week_start = da.week_start
LEFT JOIN jar_movements jm
    ON dw.dasher_id = jm.dasher_id
   AND dw.week_start = jm.week_start
LEFT JOIN weekly_end_balance web
    ON dw.dasher_id = web.dasher_id
   AND dw.week_start = web.week_start
LEFT JOIN allocation_start ast
    ON dw.dasher_id = ast.dasher_id
   AND dw.week_start = ast.week_start
LEFT JOIN allocation_end aen
    ON dw.dasher_id = aen.dasher_id
   AND dw.week_start = aen.week_start
LEFT JOIN allocation_within_week aww
    ON dw.dasher_id = aww.dasher_id
   AND dw.week_start = aww.week_start
ORDER BY dw.week_start, dw.dasher_id;
