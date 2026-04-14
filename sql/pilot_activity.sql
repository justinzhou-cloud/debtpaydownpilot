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

provider_to_dasher AS (
    SELECT
        pr.id AS provider_account_id,
        dd.dasher_id
    FROM payout_service.public.provider_accounts pr
    JOIN payout_service.public.payout_accounts pa
        ON pr.payout_account_id = pa.id
    JOIN proddb.public.dimension_dasher dd
        ON dd.payment_account_id = pa.payment_account_id
    WHERE dd.dasher_id IS NOT NULL
),

sj_transactions AS (
    SELECT
        p2d.dasher_id,
        pd.participant_name,
        t.created_at,
        DATE_TRUNC('week', t.created_at) AS week_start,
        t.transfer_type,
        t.amount / 100.0 AS amount,
        TRY_PARSE_JSON(t.metadata):ending_balance_in_cents::FLOAT / 100.0 AS end_balance
    FROM payout_service.public.money_movement_transfers t
    JOIN provider_to_dasher p2d
        ON t.provider_account_id = p2d.provider_account_id
    JOIN pilot_dashers pd
        ON p2d.dasher_id = pd.dasher_id
    WHERE t.transfer_type IN ('ManualSave', 'AutoSave', 'ManualWithdraw')
      AND t.created_at >= '2026-03-23'
)

SELECT
    dasher_id,
    participant_name,
    created_at,
    week_start,
    transfer_type,
    amount,
    end_balance
FROM sj_transactions
ORDER BY dasher_id, created_at;
