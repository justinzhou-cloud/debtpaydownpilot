-- Per-event savings jar allocation % changes (for tooltips / change dates).
-- Mirrors logic in pilot_dasher_weeks.sql allocation_events CTE.

WITH pilot_dashers AS (
    SELECT *
    FROM (VALUES
        (37091042, 'Cynthia'),
        (17709935, 'Rudy'),
        (58777515, 'Jonathan N'),
        (14042159, 'Christy'),
        (19473824, 'Efren'),
        (70682470, 'Marc Compher'),
        (59877246, 'John Baker'),
        (23609809, 'Kimberly'),
        (70847568, 'Dylan'),
        (67778641, 'Angelo'),
        (23449826, 'Crystal Shead'),
        (46601725, 'Preston')
    ) AS t(dasher_id, participant_name)
),

allocation_events AS (
    SELECT DISTINCT
        TRY_TO_NUMBER(iguazu_user_id) AS dasher_id,
        iguazu_timestamp AS event_at,
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
        iguazu_timestamp AS event_at,
        TRY_TO_NUMBER(
            TRY_PARSE_JSON(iguazu_other_properties):selected_allocation_percentage::TEXT
        ) AS allocation_pct
    FROM iguazu.driver.m_dxdr_card_action_success
    WHERE page = 'SAVINGS_JAR_TURN_OFF_AUTOMATIC_PAYOUT_SHEET'
      AND iguazu_user_id IS NOT NULL
      AND iguazu_timestamp >= '2026-01-01'
)

SELECT
    ae.dasher_id,
    pd.participant_name,
    ae.event_at,
    ae.allocation_pct
FROM allocation_events ae
INNER JOIN pilot_dashers pd
    ON ae.dasher_id = pd.dasher_id
WHERE ae.dasher_id IS NOT NULL
  AND ae.event_at >= '2026-03-23'
ORDER BY ae.dasher_id, ae.event_at;
