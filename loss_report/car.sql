  
 ---
 --- VALIDATION
 ---
 
 WITH final_salvage_events AS (
    SELECT
      iv.id AS involved_vehicle_id,
      iv.updated_at,
      iv.claim_id,
      iv.salvage_lot_number,
      'final_salvage' AS event_type,
      iv.final_salvage_amount_modified_date AS event_date,
      iv.final_salvage_amount AS amount
    FROM car_claims.involved_vehicles AS iv
    JOIN car_claims.claims AS c
        ON c.id = iv.claim_id
    WHERE 
      iv.final_salvage_amount IS NOT NULL
),

expected_salvage_events AS (
    SELECT
      iv.id AS involved_vehicle_id,
      iv.updated_at,
      iv.claim_id,
      iv.salvage_lot_number,
      'expected_salvage' AS event_type,
      iv.expected_salvage_amount_modified_date AS event_date,
      iv.expected_salvage_amount AS amount
    FROM car_claims.involved_vehicles AS iv
    JOIN car_claims.claims AS c
        ON c.id = iv.claim_id
    WHERE 
      iv.expected_salvage_amount IS NOT NULL    
),

salvage_events AS (
    SELECT  
      involved_vehicle_id,
      updated_at,
      claim_id,
      salvage_lot_number,
      event_type,
      event_date AS timestamp,
      'recovery' AS change_type,
      amount
    FROM final_salvage_events
    
    UNION ALL 
  
    SELECT 
      involved_vehicle_id,
      updated_at,
      claim_id,
      salvage_lot_number,
      event_type,
      event_date AS timestamp,
      'estimation' AS change_type,
      amount
    FROM expected_salvage_events

    UNION ALL

    SELECT 
      e.involved_vehicle_id,
      e.updated_at,
      e.claim_id,
      e.salvage_lot_number,
      e.event_type,
      f.event_date AS timestamp,
      'estimation' AS change_type,
      -1 * e.amount
    FROM expected_salvage_events AS e
    LEFT JOIN final_salvage_events AS f
      ON 
        e.involved_vehicle_id = f.involved_vehicle_id
        AND e.claim_id = f.claim_id
        AND e.salvage_lot_number = f.salvage_lot_number
    WHERE f.involved_vehicle_id IS NOT NULL
),

salvage_changes AS (
  SELECT DISTINCT
    se.*,
    ci.feature_name,
    CASE 
      WHEN ci.feature_name = 'collision' THEN 1
      WHEN ci.feature_name = 'comprehensive' THEN 2
      ELSE 3
    END rnk
  FROM salvage_events AS se
  JOIN car_claims.claim_items AS ci
    ON ci.claim_id = se.claim_id 
    AND ci.involved_vehicle_id = se.involved_vehicle_id
  QUALIFY RANK() OVER (PARTITION BY ci.claim_id ORDER BY rnk) = 1
),

loss_report AS (
    SELECT
      c.status AS status,
      'CAR'  AS line_of_business,
      CASE 
        WHEN a.feature_id = 'collision' THEN 'Auto Physical Damage'
        WHEN a.feature_id = 'comprehensive' THEN 'Auto Physical Damage'
        WHEN a.feature_id = 'property_damage_liability' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'bodily_injury_liability' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'rental' THEN 'Auto Physical Damage'
        WHEN a.feature_id = 'um_pd' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'uim_pd' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'um_bi' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'uim_bi' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'medical_payments' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'personal_injury_protection' THEN 'PP Auto No-Fault (PIP)'
        ELSE 'unknown'
      END AS reporting_code,
      a.feature_id AS feature_type,
      c.public_id AS general_claim,
      c.id AS claim_internal_id,
      '' AS catastrophe_number,
      locations.state AS state_claim_occurred,
      locations.country AS country_claim_occurred,
      CASE
        WHEN a.type IN ('paid', 'payment_failed') THEN tr.provider_type
        ELSE ''
      END AS claim_payor,
      c.incident_date AS time_of_loss,
      c.submitted_at AS report_time,
      a.type AS transaction_type,
      TO_DATE(a.change_date) AS accounting_date,
      cause.id AS cohort_id,
      REGEXP_REPLACE(cause.subtitle, '--- ', '') AS cohort,
      p.public_id AS policy,
      p.id AS policy_internal_id,
      'PP' AS form,
      address.state AS state,
      address.country AS country,
      TO_DATE(p.effective_at) AS effective_date,
      ci.expense_title AS expense_title,
      -- losses
      CASE 
        WHEN a.type IN ('paid', 'payment_failed', 'deductible_utilized', 'deductible_unutilized') 
          AND COALESCE(ci.item_type, 'loss') = 'loss' THEN -1 * a.amount
        ELSE 0
      END AS direct_losses_paid,
      0 AS salvage_received_direct,
      0 AS subro_received_direct,
      direct_losses_paid - salvage_received_direct - subro_received_direct AS losses_paid_direct_net_of_salvage_subro,
      CASE 
        WHEN COALESCE(ci.item_type, 'loss') = 'loss' THEN a.amount
        ELSE 0
      END AS unpaid_losses_reported,
      0 AS salvage_anticipated_direct,
      0 AS subro_anticipated_direct,
      unpaid_losses_reported - salvage_anticipated_direct - subro_anticipated_direct AS unpaid_losses_net_of_salvage_subro,
      losses_paid_direct_net_of_salvage_subro + unpaid_losses_net_of_salvage_subro AS losses_incurred_direct,
      -- expense
      CASE
        WHEN a.type IN ('paid', 'payment_failed', 'deductible_utilized', 'deductible_unutilized')
          AND ci.item_type = 'expense' 
          AND ci.expense_type = 'defense_and_cost_containment' THEN -1 * a.amount
        ELSE 0
      END AS defense_direct_lae_paid,
      CASE
        WHEN a.type IN ('paid', 'payment_failed', 'deductible_utilized', 'deductible_unutilized')
          AND ci.item_type = 'expense' 
          AND ci.expense_type != 'defense_and_cost_containment' THEN -1 * a.amount
        ELSE 0
      END AS adjusting_direct_lae_paid,
      defense_direct_lae_paid + adjusting_direct_lae_paid AS direct_lae_paid,
      CASE
        WHEN ci.item_type = 'expense' 
          AND ci.expense_type = 'defense_and_cost_containment' THEN a.amount
          ELSE 0
      END AS defense_direct_unpaid_lae_reported,
      CASE
        WHEN ci.item_type = 'expense' 
          AND ci.expense_type != 'defense_and_cost_containment' THEN a.amount
        ELSE 0
      END AS adjusting_direct_unpaid_lae_reported,
      defense_direct_unpaid_lae_reported + adjusting_direct_unpaid_lae_reported AS direct_unpaid_lae_reported,
      defense_direct_unpaid_lae_reported + defense_direct_lae_paid AS defense_direct_lae_incurred,
      adjusting_direct_lae_paid + adjusting_direct_unpaid_lae_reported AS adjusting_direct_lae_incurred,
      direct_lae_paid + direct_unpaid_lae_reported AS lae_incurred_direct,
      -- negation changes tracking columns
      c.updated_at::timestamp_ntz AS claims_updated_at,
      a.id AS reserve_changes_id,
      a.updated_at::timestamp_ntz AS reserve_changes_updated_at,
      ci.id AS claim_items_id,
      ci.updated_at::timestamp_ntz AS claim_items_updated_at,
      i.id AS invoices_id,
      i.updated_at::timestamp_ntz AS invoices_updated_at,
      tr.id AS transactions_id,
      tr.updated_at::timestamp_ntz AS transactions_updated_at,
      p.id AS policies_id,
      p.updated_at::timestamp_ntz AS policies_updated_at,
      q.id AS quotes_id,
      q.updated_at::timestamp_ntz AS quotes_updated_at,
      locations.id AS locations_id,
      locations.updated_at::timestamp_ntz AS locations_updated_at,
      address.id AS address_id,
      address.updated_at::timestamp_ntz AS address_updated_at,
      u.id AS users_id,
      u.updated_at::timestamp_ntz AS users_updated_at,
      cause.id AS causes_id,
      cause.updated_at::timestamp_ntz AS causes_updated_at,
      NULL AS adjuster_fees_id,
      NULL AS adjuster_fees_updated_at,
      NULL AS feature_subrogation_logs_id,
      NULL AS feature_subrogation_logs_updated_at,
      NULL AS feature_subrogations_id,
      NULL AS feature_subrogations_updated_at,
      NULL AS salvage_changes_id,
      NULL AS salvage_changes_updated_at,
      'reserve' AS section
  FROM billing.reserve_changes AS a
  JOIN car_claims.claims AS c
    ON c.public_id = a.claim_id 
  LEFT JOIN car_claims.claim_items AS ci
    ON ci.public_id = a.claim_item_id 
  LEFT JOIN billing.invoices AS i
    ON i.public_id = ci.payout_invoice_id
  LEFT JOIN billing.transactions AS tr
    ON tr.invoice_id = i.id 
    AND tr.status = 'success'
  LEFT JOIN car.policies AS p
    ON p.public_id = c.policy_public_id 
  LEFT JOIN car.quotes AS q
    ON p.quote_id = q.id
  LEFT JOIN car_claims.locations AS locations
    ON locations.id = c.incident_location_id
  LEFT JOIN car.addresses AS address
    ON address.id = q.address_id
  LEFT JOIN monolith.users AS u
    ON u.encrypted_id = c.submitting_user_public_id
  LEFT JOIN monolith.causes AS cause
    ON cause.id = u.cause_id
  WHERE 
    COALESCE(c.test, FALSE) != TRUE
    AND COALESCE(q.test, FALSE) != TRUE
  
  UNION ALL
  
  SELECT
    c.status AS status,
    'CAR'  AS line_of_business,
    CASE 
      WHEN SPLIT(a.grouping_key, '::')[0] = 'collision' THEN 'Auto Physical Damage'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'comprehensive' THEN 'Auto Physical Damage'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'property_damage_liability' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'bodily_injury_liability' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'rental' THEN 'Auto Physical Damage'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'um_pd' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'uim_pd' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'um_bi' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'uim_bi' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'medical_payments' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'personal_injury_protection' THEN 'PP Auto No-Fault (PIP)'
      ELSE 'unknown'
    END AS reporting_code,
    SPLIT(a.grouping_key, '::')[0]::string AS feature_type,
    c.public_id AS general_claim,
    c.id AS claim_internal_id,
    '' AS catastrophe_number, -- no catastrophe in car.
    locations.state AS state_claim_occurred,
    locations.country AS country_claim_occurred,
    '' AS claim_payor,
    c.incident_date AS time_of_loss,
    c.submitted_at AS report_time,
    a.event_type AS transaction_type,
    TO_DATE(a.timestamp) AS accounting_date,
    cause.id AS cohort_id,
    REGEXP_REPLACE(cause.subtitle, '--- ', '') AS cohort,
    p.public_id AS policy,
    p.id AS policy_internal_id,
    'PP' AS form,
    address.state AS state,
    address.country AS country,
    TO_DATE(p.effective_at) AS effective_date,
    NULL AS expense_title,
    -- losses
    0 AS direct_losses_paid,
    0 AS salvage_received_direct,
    0 AS subro_received_direct,
    direct_losses_paid - salvage_received_direct - subro_received_direct AS losses_paid_direct_net_of_salvage_subro,
    0 AS unpaid_losses_reported,
    0 AS salvage_anticipated_direct,
    0 AS subro_anticipated_direct,
    unpaid_losses_reported - salvage_anticipated_direct - subro_anticipated_direct AS unpaid_losses_net_of_salvage_subro,
    losses_paid_direct_net_of_salvage_subro + unpaid_losses_net_of_salvage_subro AS losses_incurred_direct,
    -- expense
    0 AS defense_direct_lae_paid,
    CASE
      WHEN a.event_type = 'payment' THEN a.amount
      ELSE 0
    END AS adjusting_direct_lae_paid,
    defense_direct_lae_paid + adjusting_direct_lae_paid AS direct_lae_paid,
    0 AS defense_direct_unpaid_lae_reported,
    CASE
      WHEN a.event_type = 'payment' THEN -1 * a.amount
      ELSE a.amount
    END AS adjusting_direct_unpaid_lae_reported,
    defense_direct_unpaid_lae_reported + adjusting_direct_unpaid_lae_reported AS direct_unpaid_lae_reported,
    defense_direct_unpaid_lae_reported + defense_direct_lae_paid AS defense_direct_lae_incurred,
    adjusting_direct_lae_paid + adjusting_direct_unpaid_lae_reported AS adjusting_direct_lae_incurred,
    direct_lae_paid + direct_unpaid_lae_reported AS lae_incurred_direct,
    -- negation changes tracking columns
    c.updated_at::timestamp_ntz AS claims_updated_at,
    NULL AS reserve_changes_id,
    NULL AS reserve_changes_updated_at,
    NULL AS claim_items_id,
    NULL AS claim_items_updated_at,
    NULL AS invoices_id,
    NULL AS invoices_updated_at,
    NULL AS transactions_id,
    NULL AS transactions_updated_at,
    p.id AS policies_id,
    p.updated_at::timestamp_ntz AS policies_updated_at,
    q.id AS quotes_id,
    q.updated_at::timestamp_ntz AS quotes_updated_at,
    locations.id AS locations_id,
    locations.updated_at::timestamp_ntz AS locations_updated_at,
    address.id AS address_id,
    address.updated_at::timestamp_ntz AS address_updated_at,
    u.id AS users_id,
    u.updated_at::timestamp_ntz AS users_updated_at,
    cause.id AS causes_id,
    cause.updated_at::timestamp_ntz AS causes_updated_at,
    a.id AS adjuster_fees_id,
    a.updated_at::timestamp_ntz AS adjuster_fees_updated_at,
    NULL AS feature_subrogation_logs_id,
    NULL AS feature_subrogation_logs_updated_at,
    NULL AS feature_subrogations_id,
    NULL AS feature_subrogations_updated_at,
    NULL AS salvage_changes_id,
    NULL AS salvage_changes_updated_at,
    'fees' AS section
  FROM clx.adjuster_fees AS a
  JOIN car_claims.claims AS c
    ON c.public_id = a.claim_public_id 
  LEFT JOIN car.policies AS p
    ON p.public_id = c.policy_public_id 
  LEFT JOIN car.quotes AS q
    ON p.quote_id = q.id
  LEFT JOIN car_claims.locations AS locations
    ON locations.id = c.incident_location_id
  LEFT JOIN car.addresses AS address
    ON address.id = q.address_id
  LEFT JOIN monolith.users AS u
    ON u.encrypted_id = c.submitting_user_public_id
  LEFT JOIN monolith.causes AS cause
    ON cause.id = u.cause_id
  WHERE 
    COALESCE(c.test, FALSE) != TRUE
    AND COALESCE(q.test, FALSE) != TRUE
  
  UNION ALL
  
  SELECT
    c.status AS status,
    'CAR'  AS line_of_business,
    CASE 
      WHEN fs.feature_type = 'collision' THEN 'Auto Physical Damage'
      WHEN fs.feature_type = 'comprehensive' THEN 'Auto Physical Damage'
      WHEN fs.feature_type = 'property_damage_liability' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'bodily_injury_liability' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'rental' THEN 'Auto Physical Damage'
      WHEN fs.feature_type = 'um_pd' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'uim_pd' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'um_bi' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'uim_bi' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'medical_payments' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'personal_injury_protection' THEN 'PP Auto No-Fault (PIP)'
      ELSE 'unknown'
    END AS reporting_code,
    fs.feature_type AS feature_type,
    c.public_id AS general_claim,
    c.id AS claim_internal_id,
    '' AS catastrophe_number, -- no catastrophe in car.
    locations.state AS state_claim_occurred,
    locations.country AS country_claim_occurred,
    '' AS claim_payor,
    c.incident_date AS time_of_loss,
    c.submitted_at AS report_time,
    a.change_type AS transaction_type,
    TO_DATE(a.timestamp) AS accounting_date,
    cause.id AS cohort_id,
    REGEXP_REPLACE(cause.subtitle, '--- ', '') AS cohort,
    p.public_id AS policy,
    p.id AS policy_internal_id,
    'PP' AS form,
    address.state AS state,
    address.country AS country,
    TO_DATE(p.effective_at) AS effective_date,
    NULL AS expense_title,
    -- losses
    0 AS direct_losses_paid,
    0 AS salvage_received_direct,
    COALESCE(a.recovered_funds_change, 0) AS subro_received_direct,
    direct_losses_paid - salvage_received_direct - subro_received_direct AS losses_paid_direct_net_of_salvage_subro,
    0 AS unpaid_losses_reported,
    0 AS salvage_anticipated_direct,
    COALESCE(a.estimated_recovery_change, 0) AS subro_anticipated_direct,
    unpaid_losses_reported - salvage_anticipated_direct - subro_anticipated_direct AS unpaid_losses_net_of_salvage_subro,
    losses_paid_direct_net_of_salvage_subro + unpaid_losses_net_of_salvage_subro AS losses_incurred_direct,
    -- expense
    0 AS defense_direct_lae_paid,
    0 AS adjusting_direct_lae_paid,
    defense_direct_lae_paid + adjusting_direct_lae_paid AS direct_lae_paid,
    0 AS defense_direct_unpaid_lae_reported,
    0 AS adjusting_direct_unpaid_lae_reported,
    defense_direct_unpaid_lae_reported + adjusting_direct_unpaid_lae_reported AS direct_unpaid_lae_reported,
    defense_direct_unpaid_lae_reported + defense_direct_lae_paid AS defense_direct_lae_incurred,
    adjusting_direct_lae_paid + adjusting_direct_unpaid_lae_reported AS adjusting_direct_lae_incurred,
    direct_lae_paid + direct_unpaid_lae_reported AS lae_incurred_direct,
    -- negation changes tracking columns
    c.updated_at::timestamp_ntz AS claims_updated_at,
    NULL AS reserve_changes_id,
    NULL AS reserve_changes_updated_at,
    NULL AS claim_items_id,
    NULL AS claim_items_updated_at,
    NULL AS invoices_id,
    NULL AS invoices_updated_at,
    NULL AS transactions_id,
    NULL AS transactions_updated_at,
    p.id AS policies_id,
    p.updated_at::timestamp_ntz AS policies_updated_at,
    q.id AS quotes_id,
    q.updated_at::timestamp_ntz AS quotes_updated_at,
    locations.id AS locations_id,
    locations.updated_at::timestamp_ntz AS locations_updated_at,
    address.id AS address_id,
    address.updated_at::timestamp_ntz AS address_updated_at,
    u.id AS users_id,
    u.updated_at::timestamp_ntz AS users_updated_at,
    cause.id AS causes_id,
    cause.updated_at::timestamp_ntz AS causes_updated_at,
    NULL AS adjuster_fees_id,
    NULL AS adjuster_fees_updated_at,
    a.id AS feature_subrogation_logs_id,
    a.updated_at::timestamp_ntz AS feature_subrogation_logs_updated_at,
    fs.id AS feature_subrogations_id,
    fs.updated_at::timestamp_ntz AS feature_subrogations_updated_at,
    NULL AS salvage_changes_id,
    NULL AS salvage_changes_updated_at,
    'subro' AS section
  FROM clx.feature_subrogation_logs AS a
  JOIN clx.feature_subrogations AS fs
    ON a.feature_subrogation_id = fs.id
  JOIN car_claims.claims AS c
    ON c.public_id = fs.claim_public_id 
  LEFT JOIN car.policies AS p
    ON p.public_id = c.policy_public_id 
  LEFT JOIN car.quotes AS q
    ON p.quote_id = q.id
  LEFT JOIN car_claims.locations AS locations
    ON locations.id = c.incident_location_id
  LEFT JOIN car.addresses AS address
    ON address.id = q.address_id
  LEFT JOIN monolith.users AS u
    ON u.encrypted_id = c.submitting_user_public_id
  LEFT JOIN monolith.causes AS cause
    ON cause.id = u.cause_id
  WHERE 
    COALESCE(c.test, FALSE) != TRUE
    AND COALESCE(q.test, FALSE) != TRUE
  
  UNION ALL
  
  SELECT
    c.status AS status,
    'CAR'  AS line_of_business,
    CASE 
      WHEN a.feature_name = 'collision' THEN 'Auto Physical Damage'
      WHEN a.feature_name = 'comprehensive' THEN 'Auto Physical Damage'
      WHEN a.feature_name = 'property_damage_liability' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'bodily_injury_liability' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'rental' THEN 'Auto Physical Damage'
      WHEN a.feature_name = 'um_pd' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'uim_pd' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'um_bi' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'uim_bi' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'medical_payments' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'personal_injury_protection' THEN 'PP Auto No-Fault (PIP)'
      ELSE 'unknown'
    END AS reporting_code,
    a.feature_name AS feature_type,
    c.public_id AS general_claim,
    c.id AS claim_internal_id,
    '' AS catastrophe_number, -- no catastrophe in car.
    locations.state AS state_claim_occurred,
    locations.country AS country_claim_occurred,
    '' AS claim_payor,
    c.incident_date AS time_of_loss,
    c.submitted_at AS report_time,
    a.change_type AS transaction_type,
    TO_DATE(a.timestamp) AS accounting_date,
    cause.id AS cohort_id,
    REGEXP_REPLACE(cause.subtitle, '--- ', '') AS cohort,
    p.public_id AS policy,
    p.id AS policy_internal_id,
    'PP' AS form,
    address.state AS state,
    address.country AS country,
    TO_DATE(p.effective_at) AS effective_date,
    NULL AS expense_title,
    -- losses
    0 AS direct_losses_paid,
    IFF(a.event_type = 'final_salvage', COALESCE(a.amount, 0), 0) AS salvage_received_direct,
    0 AS subro_received_direct,
    direct_losses_paid - salvage_received_direct - subro_received_direct AS losses_paid_direct_net_of_salvage_subro,
    0 AS unpaid_losses_reported,
    IFF(a.event_type = 'expected_salvage', COALESCE(a.amount, 0), 0) AS salvage_anticipated_direct,
    0 AS subro_anticipated_direct,
    unpaid_losses_reported - salvage_anticipated_direct - subro_anticipated_direct AS unpaid_losses_net_of_salvage_subro,
    losses_paid_direct_net_of_salvage_subro + unpaid_losses_net_of_salvage_subro AS losses_incurred_direct,
    -- expense
    0 AS defense_direct_lae_paid,
    0 AS adjusting_direct_lae_paid,
    defense_direct_lae_paid + adjusting_direct_lae_paid AS direct_lae_paid,
    0 AS defense_direct_unpaid_lae_reported,
    0 AS adjusting_direct_unpaid_lae_reported,
    defense_direct_unpaid_lae_reported + adjusting_direct_unpaid_lae_reported AS direct_unpaid_lae_reported,
    defense_direct_unpaid_lae_reported + defense_direct_lae_paid AS defense_direct_lae_incurred,
    adjusting_direct_lae_paid + adjusting_direct_unpaid_lae_reported AS adjusting_direct_lae_incurred,
    direct_lae_paid + direct_unpaid_lae_reported AS lae_incurred_direct,
    -- negation changes tracking columns
    c.updated_at::timestamp_ntz AS claims_updated_at,
    NULL AS reserve_changes_id,
    NULL AS reserve_changes_updated_at,
    NULL AS claim_items_id,
    NULL AS claim_items_updated_at,
    NULL AS invoices_id,
    NULL AS invoices_updated_at,
    NULL AS transactions_id,
    NULL AS transactions_updated_at,
    p.id AS policies_id,
    p.updated_at::timestamp_ntz AS policies_updated_at,
    q.id AS quotes_id,
    q.updated_at::timestamp_ntz AS quotes_updated_at,
    locations.id AS locations_id,
    locations.updated_at::timestamp_ntz AS locations_updated_at,
    address.id AS address_id,
    address.updated_at::timestamp_ntz AS address_updated_at,
    u.id AS users_id,
    u.updated_at::timestamp_ntz AS users_updated_at,
    cause.id AS causes_id,
    cause.updated_at::timestamp_ntz AS causes_updated_at,
    NULL AS adjuster_fees_id,
    NULL AS adjuster_fees_updated_at,
    NULL AS feature_subrogation_logs_id,
    NULL AS feature_subrogation_logs_updated_at,
    NULL AS feature_subrogations_id,
    NULL AS feature_subrogations_updated_at,
    a.involved_vehicle_id AS salvage_changes_id,
    a.updated_at::timestamp_ntz AS salvage_changes_updated_at,
    'salvage' AS section
  FROM salvage_changes AS a
  JOIN car_claims.claims AS c
    ON c.id = a.claim_id 
  LEFT JOIN car.policies AS p
    ON p.public_id = c.policy_public_id 
  LEFT JOIN car.quotes AS q
    ON p.quote_id = q.id
  LEFT JOIN car_claims.locations AS locations
    ON locations.id = c.incident_location_id
  LEFT JOIN car.addresses AS address
    ON address.id = q.address_id
  LEFT JOIN monolith.users AS u
    ON u.encrypted_id = c.submitting_user_public_id
  LEFT JOIN monolith.causes AS cause
    ON cause.id = u.cause_id
  WHERE 
    COALESCE(c.test, FALSE) != TRUE
    AND COALESCE(q.test, FALSE) != TRUE
),

metrics_based_on_source_tables AS (
  SELECT
    claim_internal_id,
    section,
    CASE
        WHEN section = 'reserve' THEN reserve_changes_id
        WHEN section = 'fees' THEN adjuster_fees_id
        WHEN section = 'subro' THEN feature_subrogation_logs_id
        WHEN section = 'salvage' THEN salvage_changes_id
    END AS unique_transaction_id,
    SUM(direct_losses_paid) AS direct_losses_paid,
    SUM(salvage_received_direct) AS salvage_received_direct,
    SUM(subro_received_direct) AS subro_received_direct,
    SUM(losses_paid_direct_net_of_salvage_subro) AS losses_paid_direct_net_of_salvage_subro,
    SUM(unpaid_losses_reported) AS unpaid_losses_reported,
    SUM(salvage_anticipated_direct) AS salvage_anticipated_direct,
    SUM(subro_anticipated_direct) AS subro_anticipated_direct,
    SUM(unpaid_losses_net_of_salvage_subro) AS unpaid_losses_net_of_salvage_subro,
    SUM(losses_incurred_direct) AS losses_incurred_direct,
    SUM(defense_direct_lae_paid) AS defense_direct_lae_paid,
    SUM(adjusting_direct_lae_paid) AS adjusting_direct_lae_paid,
    SUM(direct_lae_paid) AS direct_lae_paid,
    SUM(defense_direct_unpaid_lae_reported) AS defense_direct_unpaid_lae_reported,
    SUM(adjusting_direct_unpaid_lae_reported) AS adjusting_direct_unpaid_lae_reported,
    SUM(direct_unpaid_lae_reported) AS direct_unpaid_lae_reported,
    SUM(defense_direct_lae_incurred) AS defense_direct_lae_incurred,
    SUM(adjusting_direct_lae_incurred) AS adjusting_direct_lae_incurred,
    SUM(lae_incurred_direct) AS lae_incurred_direct
  FROM loss_report
  WHERE
    accounting_date < CURRENT_DATE
  GROUP BY 1, 2, 3
),

metrics_based_on_loss_report_table AS (
  SELECT
    claim_internal_id,
    section,
    CASE
        WHEN section = 'reserve' THEN reserve_changes_id
        WHEN section = 'fees' THEN adjuster_fees_id
        WHEN section = 'subro' THEN feature_subrogation_logs_id
        WHEN section = 'salvage' THEN salvage_changes_id
    END AS unique_transaction_id,
    SUM(direct_losses_paid) AS direct_losses_paid,
    SUM(salvage_received_direct) AS salvage_received_direct,
    SUM(subro_received_direct) AS subro_received_direct,
    SUM(losses_paid_direct_net_of_salvage_subro) AS losses_paid_direct_net_of_salvage_subro,
    SUM(unpaid_losses_reported) AS unpaid_losses_reported,
    SUM(salvage_anticipated_direct) AS salvage_anticipated_direct,
    SUM(subro_anticipated_direct) AS subro_anticipated_direct,
    SUM(unpaid_losses_net_of_salvage_subro) AS unpaid_losses_net_of_salvage_subro,
    SUM(losses_incurred_direct) AS losses_incurred_direct,
    SUM(defense_direct_lae_paid) AS defense_direct_lae_paid,
    SUM(adjusting_direct_lae_paid) AS adjusting_direct_lae_paid,
    SUM(direct_lae_paid) AS direct_lae_paid,
    SUM(defense_direct_unpaid_lae_reported) AS defense_direct_unpaid_lae_reported,
    SUM(adjusting_direct_unpaid_lae_reported) AS adjusting_direct_unpaid_lae_reported,
    SUM(direct_unpaid_lae_reported) AS direct_unpaid_lae_reported,
    SUM(defense_direct_lae_incurred) AS defense_direct_lae_incurred,
    SUM(adjusting_direct_lae_incurred) AS adjusting_direct_lae_incurred,
    SUM(lae_incurred_direct) AS lae_incurred_direct
  FROM car_finance.loss_report_raw
  --FROM  lemonade.backups.CAR_FINANCE_LOSS_REPORT_RAW_2022_08_16
  WHERE
    accounting_date <= CURRENT_DATE
  GROUP BY 1, 2, 3
),

diff AS (
    SELECT *
    FROM metrics_based_on_source_tables

    MINUS

    SELECT *
    FROM metrics_based_on_loss_report_table
)

SELECT * -- COUNT(1) AS invalid_records_count
FROM diff AS d;
/*
--1115	salvage	1335
954	salvage	1142
--961	salvage	1149
*/
;;;

select *
from car_finance.loss_report_raw
where 
    claim_internal_id = 954 
    and section = 'salvage'
    and unpaid_losses_net_of_salvage_subro in (0,-579)
    order by created_at desc;

;;;

SELECT *
FROM lemonade.backups.CAR_FINANCE_LOSS_REPORT_RAW_2022_08_16
WHERE section = 'salvage'
    and claim_internal_id = 954
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY 
        claim_internal_id, 
        salvage_changes_id 
    ORDER BY 
        accounting_date DESC,
        created_at DESC,
        transaction_type DESC
) = 1

;;;

--- NEGATE ---Table CAR_FINANCE_LOSS_REPORT_RAW_2022_08_16 successfully created.


INSERT INTO car_finance.loss_report_raw(
   status, line_of_business, reporting_code, feature_type, general_claim, claim_internal_id,
   catastrophe_number, state_claim_occurred, country_claim_occurred, claim_payor, time_of_loss,
   report_time, transaction_type, accounting_date, cohort_id, cohort, policy, policy_internal_id,
   form, state, country, effective_date, expense_title, direct_losses_paid, salvage_received_direct,
   subro_received_direct, losses_paid_direct_net_of_salvage_subro, unpaid_losses_reported,
   salvage_anticipated_direct, subro_anticipated_direct, unpaid_losses_net_of_salvage_subro, 
   losses_incurred_direct, defense_direct_lae_paid, adjusting_direct_lae_paid, direct_lae_paid,
   defense_direct_unpaid_lae_reported, adjusting_direct_unpaid_lae_reported, direct_unpaid_lae_reported,
   defense_direct_lae_incurred, adjusting_direct_lae_incurred, lae_incurred_direct, claims_updated_at,
   reserve_changes_id, reserve_changes_updated_at, claim_items_id, claim_items_updated_at, invoices_id, 
   invoices_updated_at, transactions_id, transactions_updated_at, policies_id, policies_updated_at, 
   quotes_id, quotes_updated_at, locations_id, locations_updated_at, address_id, address_updated_at, 
   users_id, users_updated_at, causes_id, causes_updated_at, adjuster_fees_id, adjuster_fees_updated_at, 
   feature_subrogation_logs_id, feature_subrogation_logs_updated_at, feature_subrogations_id, 
   feature_subrogations_updated_at, salvage_changes_id, salvage_changes_updated_at, section, 
   created_at, record_type, comments
)

with duplicates AS (
    select *
    from car_finance.loss_report_raw
    where --954	salvage	1142
        claim_internal_id = 954
        and salvage_changes_id = 1142
        and record_type = 'new'
        and section = 'salvage'
        and unpaid_losses_net_of_salvage_subro in (0,-579)
)

SELECT 
  loss_report.status, 
  loss_report.line_of_business, 
  loss_report.reporting_code, 
  loss_report.feature_type, 
  loss_report.general_claim, 
  loss_report.claim_internal_id,
  loss_report.catastrophe_number, 
  loss_report.state_claim_occurred, 
  loss_report.country_claim_occurred, 
  loss_report.claim_payor, time_of_loss,
  loss_report.report_time, 
  loss_report.transaction_type, 
  CURRENT_TIMESTAMP::timestamp_ntz AS accounting_date, 
  loss_report.cohort_id, 
  loss_report.cohort, 
  loss_report.policy, 
  loss_report.policy_internal_id,
  loss_report.form, 
  loss_report.state, 
  loss_report.country, 
  loss_report.effective_date, 
  loss_report.expense_title, 
  -1 * loss_report.direct_losses_paid, 
  -1 * loss_report.salvage_received_direct,
  -1 * loss_report.subro_received_direct, 
  -1 * loss_report.losses_paid_direct_net_of_salvage_subro, 
  -1 * loss_report.unpaid_losses_reported,
  -1 * loss_report.salvage_anticipated_direct, 
  -1 * loss_report.subro_anticipated_direct, 
  -1 * loss_report.unpaid_losses_net_of_salvage_subro, 
  -1 * loss_report.losses_incurred_direct, 
  -1 * loss_report.defense_direct_lae_paid, 
  -1 * loss_report.adjusting_direct_lae_paid, 
  -1 * loss_report.direct_lae_paid,
  -1 * loss_report.defense_direct_unpaid_lae_reported, 
  -1 * loss_report.adjusting_direct_unpaid_lae_reported, 
  -1 * loss_report.direct_unpaid_lae_reported,
  -1 * loss_report.defense_direct_lae_incurred, 
  -1 * loss_report.adjusting_direct_lae_incurred, 
  -1 * loss_report.lae_incurred_direct, 
  loss_report.claims_updated_at,
  loss_report.reserve_changes_id, 
  loss_report.reserve_changes_updated_at, 
  loss_report.claim_items_id, 
  loss_report.claim_items_updated_at, 
  loss_report.invoices_id, 
  loss_report.invoices_updated_at, 
  loss_report.transactions_id, 
  loss_report.transactions_updated_at, 
  loss_report.policies_id, 
  loss_report.policies_updated_at, 
  loss_report.quotes_id, 
  loss_report.quotes_updated_at, 
  loss_report.locations_id, 
  loss_report.locations_updated_at, 
  loss_report.address_id, 
  loss_report.address_updated_at, 
  loss_report.users_id, 
  loss_report.users_updated_at, 
  loss_report.causes_id, 
  loss_report.causes_updated_at, 
  loss_report.adjuster_fees_id, 
  loss_report.adjuster_fees_updated_at, 
  loss_report.feature_subrogation_logs_id, 
  loss_report.feature_subrogation_logs_updated_at, 
  loss_report.feature_subrogations_id, 
  loss_report.feature_subrogations_updated_at, 
  loss_report.salvage_changes_id, 
  loss_report.salvage_changes_updated_at, 
  loss_report.section AS section, 
  CURRENT_TIMESTAMP::timestamp_ntz AS created_at, 
  'negation' AS record_type, 
  object_construct('reason', 'fix duplicated record originaly created on \'2022-08-16\', using additional negation recrods', 'type', 'bug_fix') AS comments
FROM duplicates AS loss_report
limit 100;

;;;


--- UPDATE ---


INSERT INTO car_finance.loss_report_raw (
   status, line_of_business, reporting_code, feature_type, general_claim, claim_internal_id,
   catastrophe_number, state_claim_occurred, country_claim_occurred, claim_payor, time_of_loss,
   report_time, transaction_type, accounting_date, cohort_id, cohort, policy, policy_internal_id,
   form, state, country, effective_date, expense_title, direct_losses_paid, salvage_received_direct,
   subro_received_direct, losses_paid_direct_net_of_salvage_subro, unpaid_losses_reported,
   salvage_anticipated_direct, subro_anticipated_direct, unpaid_losses_net_of_salvage_subro, 
   losses_incurred_direct, defense_direct_lae_paid, adjusting_direct_lae_paid, direct_lae_paid,
   defense_direct_unpaid_lae_reported, adjusting_direct_unpaid_lae_reported, direct_unpaid_lae_reported,
   defense_direct_lae_incurred, adjusting_direct_lae_incurred, lae_incurred_direct, claims_updated_at,
   reserve_changes_id, reserve_changes_updated_at, claim_items_id, claim_items_updated_at, invoices_id, 
   invoices_updated_at, transactions_id, transactions_updated_at, policies_id, policies_updated_at, 
   quotes_id, quotes_updated_at, locations_id, locations_updated_at, address_id, address_updated_at, 
   users_id, users_updated_at, causes_id, causes_updated_at, adjuster_fees_id, adjuster_fees_updated_at, 
   feature_subrogation_logs_id, feature_subrogation_logs_updated_at, feature_subrogations_id, 
   feature_subrogations_updated_at, salvage_changes_id, salvage_changes_updated_at, section, 
   created_at, record_type, comments
)


WITH final_salvage_events AS (
    SELECT
      iv.id AS involved_vehicle_id,
      iv.updated_at,
      iv.claim_id,
      iv.salvage_lot_number,
      'final_salvage' AS event_type,
      iv.final_salvage_amount_modified_date AS event_date,
      iv.final_salvage_amount AS amount
    FROM car_claims.involved_vehicles AS iv
    JOIN car_claims.claims AS c
        ON c.id = iv.claim_id
    WHERE 
      iv.final_salvage_amount IS NOT NULL
),

expected_salvage_events AS (
    SELECT
      iv.id AS involved_vehicle_id,
      iv.updated_at,
      iv.claim_id,
      iv.salvage_lot_number,
      'expected_salvage' AS event_type,
      iv.expected_salvage_amount_modified_date AS event_date,
      iv.expected_salvage_amount AS amount
    FROM car_claims.involved_vehicles AS iv
    JOIN car_claims.claims AS c
        ON c.id = iv.claim_id
    WHERE 
      iv.expected_salvage_amount IS NOT NULL    
),

salvage_events AS (
    SELECT  
      involved_vehicle_id,
      updated_at,
      claim_id,
      salvage_lot_number,
      event_type,
      event_date AS timestamp,
      'recovery' AS change_type,
      amount
    FROM final_salvage_events
    
    UNION ALL 
  
    SELECT 
      involved_vehicle_id,
      updated_at,
      claim_id,
      salvage_lot_number,
      event_type,
      event_date AS timestamp,
      'estimation' AS change_type,
      amount
    FROM expected_salvage_events

    UNION ALL

    SELECT 
      e.involved_vehicle_id,
      e.updated_at,
      e.claim_id,
      e.salvage_lot_number,
      e.event_type,
      f.event_date AS timestamp,
      'estimation' AS change_type,
      -1 * e.amount
    FROM expected_salvage_events AS e
    LEFT JOIN final_salvage_events AS f
      ON 
        e.involved_vehicle_id = f.involved_vehicle_id
        AND e.claim_id = f.claim_id
        AND e.salvage_lot_number = f.salvage_lot_number
    WHERE f.involved_vehicle_id IS NOT NULL
),

salvage_changes AS (
  SELECT DISTINCT
    se.*,
    ci.feature_name,
    CASE 
      WHEN ci.feature_name = 'collision' THEN 1
      WHEN ci.feature_name = 'comprehensive' THEN 2
      ELSE 3
    END rnk
  FROM salvage_events AS se
  JOIN car_claims.claim_items AS ci
    ON ci.claim_id = se.claim_id 
    AND ci.involved_vehicle_id = se.involved_vehicle_id
  QUALIFY RANK() OVER (PARTITION BY ci.claim_id ORDER BY rnk) = 1
),

loss_report AS (
    SELECT
      c.status AS status,
      'CAR'  AS line_of_business,
      CASE 
        WHEN a.feature_id = 'collision' THEN 'Auto Physical Damage'
        WHEN a.feature_id = 'comprehensive' THEN 'Auto Physical Damage'
        WHEN a.feature_id = 'property_damage_liability' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'bodily_injury_liability' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'rental' THEN 'Auto Physical Damage'
        WHEN a.feature_id = 'um_pd' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'uim_pd' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'um_bi' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'uim_bi' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'medical_payments' THEN 'Other PP Auto Liability'
        WHEN a.feature_id = 'personal_injury_protection' THEN 'PP Auto No-Fault (PIP)'
        ELSE 'unknown'
      END AS reporting_code,
      a.feature_id AS feature_type,
      c.public_id AS general_claim,
      c.id AS claim_internal_id,
      '' AS catastrophe_number,
      locations.state AS state_claim_occurred,
      locations.country AS country_claim_occurred,
      CASE
        WHEN a.type IN ('paid', 'payment_failed') THEN tr.provider_type
        ELSE ''
      END AS claim_payor,
      c.incident_date AS time_of_loss,
      c.submitted_at AS report_time,
      a.type AS transaction_type,
      TO_DATE(a.change_date) AS accounting_date,
      cause.id AS cohort_id,
      REGEXP_REPLACE(cause.subtitle, '--- ', '') AS cohort,
      p.public_id AS policy,
      p.id AS policy_internal_id,
      'PP' AS form,
      address.state AS state,
      address.country AS country,
      TO_DATE(p.effective_at) AS effective_date,
      ci.expense_title AS expense_title,
      -- losses
      CASE 
        WHEN a.type IN ('paid', 'payment_failed', 'deductible_utilized', 'deductible_unutilized') 
          AND COALESCE(ci.item_type, 'loss') = 'loss' THEN -1 * a.amount
        ELSE 0
      END AS direct_losses_paid,
      0 AS salvage_received_direct,
      0 AS subro_received_direct,
      direct_losses_paid - salvage_received_direct - subro_received_direct AS losses_paid_direct_net_of_salvage_subro,
      CASE 
        WHEN COALESCE(ci.item_type, 'loss') = 'loss' THEN a.amount
        ELSE 0
      END AS unpaid_losses_reported,
      0 AS salvage_anticipated_direct,
      0 AS subro_anticipated_direct,
      unpaid_losses_reported - salvage_anticipated_direct - subro_anticipated_direct AS unpaid_losses_net_of_salvage_subro,
      losses_paid_direct_net_of_salvage_subro + unpaid_losses_net_of_salvage_subro AS losses_incurred_direct,
      -- expense
      CASE
        WHEN a.type IN ('paid', 'payment_failed', 'deductible_utilized', 'deductible_unutilized')
          AND ci.item_type = 'expense' 
          AND ci.expense_type = 'defense_and_cost_containment' THEN -1 * a.amount
        ELSE 0
      END AS defense_direct_lae_paid,
      CASE
        WHEN a.type IN ('paid', 'payment_failed', 'deductible_utilized', 'deductible_unutilized')
          AND ci.item_type = 'expense' 
          AND ci.expense_type != 'defense_and_cost_containment' THEN -1 * a.amount
        ELSE 0
      END AS adjusting_direct_lae_paid,
      defense_direct_lae_paid + adjusting_direct_lae_paid AS direct_lae_paid,
      CASE
        WHEN ci.item_type = 'expense' 
          AND ci.expense_type = 'defense_and_cost_containment' THEN a.amount
          ELSE 0
      END AS defense_direct_unpaid_lae_reported,
      CASE
        WHEN ci.item_type = 'expense' 
          AND ci.expense_type != 'defense_and_cost_containment' THEN a.amount
        ELSE 0
      END AS adjusting_direct_unpaid_lae_reported,
      defense_direct_unpaid_lae_reported + adjusting_direct_unpaid_lae_reported AS direct_unpaid_lae_reported,
      defense_direct_unpaid_lae_reported + defense_direct_lae_paid AS defense_direct_lae_incurred,
      adjusting_direct_lae_paid + adjusting_direct_unpaid_lae_reported AS adjusting_direct_lae_incurred,
      direct_lae_paid + direct_unpaid_lae_reported AS lae_incurred_direct,
      -- negation changes tracking columns
      c.updated_at::timestamp_ntz AS claims_updated_at,
      a.id AS reserve_changes_id,
      a.updated_at::timestamp_ntz AS reserve_changes_updated_at,
      ci.id AS claim_items_id,
      ci.updated_at::timestamp_ntz AS claim_items_updated_at,
      i.id AS invoices_id,
      i.updated_at::timestamp_ntz AS invoices_updated_at,
      tr.id AS transactions_id,
      tr.updated_at::timestamp_ntz AS transactions_updated_at,
      p.id AS policies_id,
      p.updated_at::timestamp_ntz AS policies_updated_at,
      q.id AS quotes_id,
      q.updated_at::timestamp_ntz AS quotes_updated_at,
      locations.id AS locations_id,
      locations.updated_at::timestamp_ntz AS locations_updated_at,
      address.id AS address_id,
      address.updated_at::timestamp_ntz AS address_updated_at,
      u.id AS users_id,
      u.updated_at::timestamp_ntz AS users_updated_at,
      cause.id AS causes_id,
      cause.updated_at::timestamp_ntz AS causes_updated_at,
      NULL AS adjuster_fees_id,
      NULL AS adjuster_fees_updated_at,
      NULL AS feature_subrogation_logs_id,
      NULL AS feature_subrogation_logs_updated_at,
      NULL AS feature_subrogations_id,
      NULL AS feature_subrogations_updated_at,
      NULL AS salvage_changes_id,
      NULL AS salvage_changes_updated_at,
      'reserve' AS section
  FROM billing.reserve_changes AS a
  JOIN car_claims.claims AS c
    ON c.public_id = a.claim_id 
  LEFT JOIN car_claims.claim_items AS ci
    ON ci.public_id = a.claim_item_id 
  LEFT JOIN billing.invoices AS i
    ON i.public_id = ci.payout_invoice_id
  LEFT JOIN billing.transactions AS tr
    ON tr.invoice_id = i.id 
    AND tr.status = 'success'
  LEFT JOIN car.policies AS p
    ON p.public_id = c.policy_public_id 
  LEFT JOIN car.quotes AS q
    ON p.quote_id = q.id
  LEFT JOIN car_claims.locations AS locations
    ON locations.id = c.incident_location_id
  LEFT JOIN car.addresses AS address
    ON address.id = q.address_id
  LEFT JOIN monolith.users AS u
    ON u.encrypted_id = c.submitting_user_public_id
  LEFT JOIN monolith.causes AS cause
    ON cause.id = u.cause_id
  WHERE 
    COALESCE(c.test, FALSE) != TRUE
    AND COALESCE(q.test, FALSE) != TRUE
  
  UNION ALL
  
  SELECT
    c.status AS status,
    'CAR'  AS line_of_business,
    CASE 
      WHEN SPLIT(a.grouping_key, '::')[0] = 'collision' THEN 'Auto Physical Damage'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'comprehensive' THEN 'Auto Physical Damage'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'property_damage_liability' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'bodily_injury_liability' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'rental' THEN 'Auto Physical Damage'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'um_pd' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'uim_pd' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'um_bi' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'uim_bi' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'medical_payments' THEN 'Other PP Auto Liability'
      WHEN SPLIT(a.grouping_key, '::')[0] = 'personal_injury_protection' THEN 'PP Auto No-Fault (PIP)'
      ELSE 'unknown'
    END AS reporting_code,
    SPLIT(a.grouping_key, '::')[0]::string AS feature_type,
    c.public_id AS general_claim,
    c.id AS claim_internal_id,
    '' AS catastrophe_number, -- no catastrophe in car.
    locations.state AS state_claim_occurred,
    locations.country AS country_claim_occurred,
    '' AS claim_payor,
    c.incident_date AS time_of_loss,
    c.submitted_at AS report_time,
    a.event_type AS transaction_type,
    TO_DATE(a.timestamp) AS accounting_date,
    cause.id AS cohort_id,
    REGEXP_REPLACE(cause.subtitle, '--- ', '') AS cohort,
    p.public_id AS policy,
    p.id AS policy_internal_id,
    'PP' AS form,
    address.state AS state,
    address.country AS country,
    TO_DATE(p.effective_at) AS effective_date,
    NULL AS expense_title,
    -- losses
    0 AS direct_losses_paid,
    0 AS salvage_received_direct,
    0 AS subro_received_direct,
    direct_losses_paid - salvage_received_direct - subro_received_direct AS losses_paid_direct_net_of_salvage_subro,
    0 AS unpaid_losses_reported,
    0 AS salvage_anticipated_direct,
    0 AS subro_anticipated_direct,
    unpaid_losses_reported - salvage_anticipated_direct - subro_anticipated_direct AS unpaid_losses_net_of_salvage_subro,
    losses_paid_direct_net_of_salvage_subro + unpaid_losses_net_of_salvage_subro AS losses_incurred_direct,
    -- expense
    0 AS defense_direct_lae_paid,
    CASE
      WHEN a.event_type = 'payment' THEN a.amount
      ELSE 0
    END AS adjusting_direct_lae_paid,
    defense_direct_lae_paid + adjusting_direct_lae_paid AS direct_lae_paid,
    0 AS defense_direct_unpaid_lae_reported,
    CASE
      WHEN a.event_type = 'payment' THEN -1 * a.amount
      ELSE a.amount
    END AS adjusting_direct_unpaid_lae_reported,
    defense_direct_unpaid_lae_reported + adjusting_direct_unpaid_lae_reported AS direct_unpaid_lae_reported,
    defense_direct_unpaid_lae_reported + defense_direct_lae_paid AS defense_direct_lae_incurred,
    adjusting_direct_lae_paid + adjusting_direct_unpaid_lae_reported AS adjusting_direct_lae_incurred,
    direct_lae_paid + direct_unpaid_lae_reported AS lae_incurred_direct,
    -- negation changes tracking columns
    c.updated_at::timestamp_ntz AS claims_updated_at,
    NULL AS reserve_changes_id,
    NULL AS reserve_changes_updated_at,
    NULL AS claim_items_id,
    NULL AS claim_items_updated_at,
    NULL AS invoices_id,
    NULL AS invoices_updated_at,
    NULL AS transactions_id,
    NULL AS transactions_updated_at,
    p.id AS policies_id,
    p.updated_at::timestamp_ntz AS policies_updated_at,
    q.id AS quotes_id,
    q.updated_at::timestamp_ntz AS quotes_updated_at,
    locations.id AS locations_id,
    locations.updated_at::timestamp_ntz AS locations_updated_at,
    address.id AS address_id,
    address.updated_at::timestamp_ntz AS address_updated_at,
    u.id AS users_id,
    u.updated_at::timestamp_ntz AS users_updated_at,
    cause.id AS causes_id,
    cause.updated_at::timestamp_ntz AS causes_updated_at,
    a.id AS adjuster_fees_id,
    a.updated_at::timestamp_ntz AS adjuster_fees_updated_at,
    NULL AS feature_subrogation_logs_id,
    NULL AS feature_subrogation_logs_updated_at,
    NULL AS feature_subrogations_id,
    NULL AS feature_subrogations_updated_at,
    NULL AS salvage_changes_id,
    NULL AS salvage_changes_updated_at,
    'fees' AS section
  FROM clx.adjuster_fees AS a
  JOIN car_claims.claims AS c
    ON c.public_id = a.claim_public_id 
  LEFT JOIN car.policies AS p
    ON p.public_id = c.policy_public_id 
  LEFT JOIN car.quotes AS q
    ON p.quote_id = q.id
  LEFT JOIN car_claims.locations AS locations
    ON locations.id = c.incident_location_id
  LEFT JOIN car.addresses AS address
    ON address.id = q.address_id
  LEFT JOIN monolith.users AS u
    ON u.encrypted_id = c.submitting_user_public_id
  LEFT JOIN monolith.causes AS cause
    ON cause.id = u.cause_id
  WHERE 
    COALESCE(c.test, FALSE) != TRUE
    AND COALESCE(q.test, FALSE) != TRUE
  
  UNION ALL
  
  SELECT
    c.status AS status,
    'CAR'  AS line_of_business,
    CASE 
      WHEN fs.feature_type = 'collision' THEN 'Auto Physical Damage'
      WHEN fs.feature_type = 'comprehensive' THEN 'Auto Physical Damage'
      WHEN fs.feature_type = 'property_damage_liability' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'bodily_injury_liability' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'rental' THEN 'Auto Physical Damage'
      WHEN fs.feature_type = 'um_pd' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'uim_pd' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'um_bi' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'uim_bi' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'medical_payments' THEN 'Other PP Auto Liability'
      WHEN fs.feature_type = 'personal_injury_protection' THEN 'PP Auto No-Fault (PIP)'
      ELSE 'unknown'
    END AS reporting_code,
    fs.feature_type AS feature_type,
    c.public_id AS general_claim,
    c.id AS claim_internal_id,
    '' AS catastrophe_number, -- no catastrophe in car.
    locations.state AS state_claim_occurred,
    locations.country AS country_claim_occurred,
    '' AS claim_payor,
    c.incident_date AS time_of_loss,
    c.submitted_at AS report_time,
    a.change_type AS transaction_type,
    TO_DATE(a.timestamp) AS accounting_date,
    cause.id AS cohort_id,
    REGEXP_REPLACE(cause.subtitle, '--- ', '') AS cohort,
    p.public_id AS policy,
    p.id AS policy_internal_id,
    'PP' AS form,
    address.state AS state,
    address.country AS country,
    TO_DATE(p.effective_at) AS effective_date,
    NULL AS expense_title,
    -- losses
    0 AS direct_losses_paid,
    0 AS salvage_received_direct,
    COALESCE(a.recovered_funds_change, 0) AS subro_received_direct,
    direct_losses_paid - salvage_received_direct - subro_received_direct AS losses_paid_direct_net_of_salvage_subro,
    0 AS unpaid_losses_reported,
    0 AS salvage_anticipated_direct,
    COALESCE(a.estimated_recovery_change, 0) AS subro_anticipated_direct,
    unpaid_losses_reported - salvage_anticipated_direct - subro_anticipated_direct AS unpaid_losses_net_of_salvage_subro,
    losses_paid_direct_net_of_salvage_subro + unpaid_losses_net_of_salvage_subro AS losses_incurred_direct,
    -- expense
    0 AS defense_direct_lae_paid,
    0 AS adjusting_direct_lae_paid,
    defense_direct_lae_paid + adjusting_direct_lae_paid AS direct_lae_paid,
    0 AS defense_direct_unpaid_lae_reported,
    0 AS adjusting_direct_unpaid_lae_reported,
    defense_direct_unpaid_lae_reported + adjusting_direct_unpaid_lae_reported AS direct_unpaid_lae_reported,
    defense_direct_unpaid_lae_reported + defense_direct_lae_paid AS defense_direct_lae_incurred,
    adjusting_direct_lae_paid + adjusting_direct_unpaid_lae_reported AS adjusting_direct_lae_incurred,
    direct_lae_paid + direct_unpaid_lae_reported AS lae_incurred_direct,
    -- negation changes tracking columns
    c.updated_at::timestamp_ntz AS claims_updated_at,
    NULL AS reserve_changes_id,
    NULL AS reserve_changes_updated_at,
    NULL AS claim_items_id,
    NULL AS claim_items_updated_at,
    NULL AS invoices_id,
    NULL AS invoices_updated_at,
    NULL AS transactions_id,
    NULL AS transactions_updated_at,
    p.id AS policies_id,
    p.updated_at::timestamp_ntz AS policies_updated_at,
    q.id AS quotes_id,
    q.updated_at::timestamp_ntz AS quotes_updated_at,
    locations.id AS locations_id,
    locations.updated_at::timestamp_ntz AS locations_updated_at,
    address.id AS address_id,
    address.updated_at::timestamp_ntz AS address_updated_at,
    u.id AS users_id,
    u.updated_at::timestamp_ntz AS users_updated_at,
    cause.id AS causes_id,
    cause.updated_at::timestamp_ntz AS causes_updated_at,
    NULL AS adjuster_fees_id,
    NULL AS adjuster_fees_updated_at,
    a.id AS feature_subrogation_logs_id,
    a.updated_at::timestamp_ntz AS feature_subrogation_logs_updated_at,
    fs.id AS feature_subrogations_id,
    fs.updated_at::timestamp_ntz AS feature_subrogations_updated_at,
    NULL AS salvage_changes_id,
    NULL AS salvage_changes_updated_at,
    'subro' AS section
  FROM clx.feature_subrogation_logs AS a
  JOIN clx.feature_subrogations AS fs
    ON a.feature_subrogation_id = fs.id
  JOIN car_claims.claims AS c
    ON c.public_id = fs.claim_public_id 
  LEFT JOIN car.policies AS p
    ON p.public_id = c.policy_public_id 
  LEFT JOIN car.quotes AS q
    ON p.quote_id = q.id
  LEFT JOIN car_claims.locations AS locations
    ON locations.id = c.incident_location_id
  LEFT JOIN car.addresses AS address
    ON address.id = q.address_id
  LEFT JOIN monolith.users AS u
    ON u.encrypted_id = c.submitting_user_public_id
  LEFT JOIN monolith.causes AS cause
    ON cause.id = u.cause_id
  WHERE 
    COALESCE(c.test, FALSE) != TRUE
    AND COALESCE(q.test, FALSE) != TRUE
  
  UNION ALL
  
  SELECT
    c.status AS status,
    'CAR'  AS line_of_business,
    CASE 
      WHEN a.feature_name = 'collision' THEN 'Auto Physical Damage'
      WHEN a.feature_name = 'comprehensive' THEN 'Auto Physical Damage'
      WHEN a.feature_name = 'property_damage_liability' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'bodily_injury_liability' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'rental' THEN 'Auto Physical Damage'
      WHEN a.feature_name = 'um_pd' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'uim_pd' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'um_bi' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'uim_bi' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'medical_payments' THEN 'Other PP Auto Liability'
      WHEN a.feature_name = 'personal_injury_protection' THEN 'PP Auto No-Fault (PIP)'
      ELSE 'unknown'
    END AS reporting_code,
    a.feature_name AS feature_type,
    c.public_id AS general_claim,
    c.id AS claim_internal_id,
    '' AS catastrophe_number, -- no catastrophe in car.
    locations.state AS state_claim_occurred,
    locations.country AS country_claim_occurred,
    '' AS claim_payor,
    c.incident_date AS time_of_loss,
    c.submitted_at AS report_time,
    a.change_type AS transaction_type,
    TO_DATE(a.timestamp) AS accounting_date,
    cause.id AS cohort_id,
    REGEXP_REPLACE(cause.subtitle, '--- ', '') AS cohort,
    p.public_id AS policy,
    p.id AS policy_internal_id,
    'PP' AS form,
    address.state AS state,
    address.country AS country,
    TO_DATE(p.effective_at) AS effective_date,
    NULL AS expense_title,
    -- losses
    0 AS direct_losses_paid,
    IFF(a.event_type = 'final_salvage', COALESCE(a.amount, 0), 0) AS salvage_received_direct,
    0 AS subro_received_direct,
    direct_losses_paid - salvage_received_direct - subro_received_direct AS losses_paid_direct_net_of_salvage_subro,
    0 AS unpaid_losses_reported,
    IFF(a.event_type = 'expected_salvage', COALESCE(a.amount, 0), 0) AS salvage_anticipated_direct,
    0 AS subro_anticipated_direct,
    unpaid_losses_reported - salvage_anticipated_direct - subro_anticipated_direct AS unpaid_losses_net_of_salvage_subro,
    losses_paid_direct_net_of_salvage_subro + unpaid_losses_net_of_salvage_subro AS losses_incurred_direct,
    -- expense
    0 AS defense_direct_lae_paid,
    0 AS adjusting_direct_lae_paid,
    defense_direct_lae_paid + adjusting_direct_lae_paid AS direct_lae_paid,
    0 AS defense_direct_unpaid_lae_reported,
    0 AS adjusting_direct_unpaid_lae_reported,
    defense_direct_unpaid_lae_reported + adjusting_direct_unpaid_lae_reported AS direct_unpaid_lae_reported,
    defense_direct_unpaid_lae_reported + defense_direct_lae_paid AS defense_direct_lae_incurred,
    adjusting_direct_lae_paid + adjusting_direct_unpaid_lae_reported AS adjusting_direct_lae_incurred,
    direct_lae_paid + direct_unpaid_lae_reported AS lae_incurred_direct,
    -- negation changes tracking columns
    c.updated_at::timestamp_ntz AS claims_updated_at,
    NULL AS reserve_changes_id,
    NULL AS reserve_changes_updated_at,
    NULL AS claim_items_id,
    NULL AS claim_items_updated_at,
    NULL AS invoices_id,
    NULL AS invoices_updated_at,
    NULL AS transactions_id,
    NULL AS transactions_updated_at,
    p.id AS policies_id,
    p.updated_at::timestamp_ntz AS policies_updated_at,
    q.id AS quotes_id,
    q.updated_at::timestamp_ntz AS quotes_updated_at,
    locations.id AS locations_id,
    locations.updated_at::timestamp_ntz AS locations_updated_at,
    address.id AS address_id,
    address.updated_at::timestamp_ntz AS address_updated_at,
    u.id AS users_id,
    u.updated_at::timestamp_ntz AS users_updated_at,
    cause.id AS causes_id,
    cause.updated_at::timestamp_ntz AS causes_updated_at,
    NULL AS adjuster_fees_id,
    NULL AS adjuster_fees_updated_at,
    NULL AS feature_subrogation_logs_id,
    NULL AS feature_subrogation_logs_updated_at,
    NULL AS feature_subrogations_id,
    NULL AS feature_subrogations_updated_at,
    a.involved_vehicle_id AS salvage_changes_id,
    a.updated_at::timestamp_ntz AS salvage_changes_updated_at,
    'salvage' AS section
  FROM salvage_changes AS a
  JOIN car_claims.claims AS c
    ON c.id = a.claim_id 
  LEFT JOIN car.policies AS p
    ON p.public_id = c.policy_public_id 
  LEFT JOIN car.quotes AS q
    ON p.quote_id = q.id
  LEFT JOIN car_claims.locations AS locations
    ON locations.id = c.incident_location_id
  LEFT JOIN car.addresses AS address
    ON address.id = q.address_id
  LEFT JOIN monolith.users AS u
    ON u.encrypted_id = c.submitting_user_public_id
  LEFT JOIN monolith.causes AS cause
    ON cause.id = u.cause_id
  WHERE 
    COALESCE(c.test, FALSE) != TRUE
    AND COALESCE(q.test, FALSE) != TRUE
)

SELECT  
  loss_report.status, 
  loss_report.line_of_business, 
  loss_report.reporting_code, 
  loss_report.feature_type, 
  loss_report.general_claim, 
  loss_report.claim_internal_id,
  loss_report.catastrophe_number, 
  loss_report.state_claim_occurred, 
  loss_report.country_claim_occurred, 
  loss_report.claim_payor, time_of_loss,
  loss_report.report_time, 
  loss_report.transaction_type, 
  CURRENT_TIMESTAMP::timestamp_ntz AS accounting_date, 
  loss_report.cohort_id, 
  loss_report.cohort, 
  loss_report.policy, 
  loss_report.policy_internal_id,
  loss_report.form, 
  loss_report.state, 
  loss_report.country, 
  loss_report.effective_date, 
  loss_report.expense_title, 
  loss_report.direct_losses_paid, 
  loss_report.salvage_received_direct,
  loss_report.subro_received_direct, 
  loss_report.losses_paid_direct_net_of_salvage_subro, 
  loss_report.unpaid_losses_reported,
  loss_report.salvage_anticipated_direct, 
  loss_report.subro_anticipated_direct, 
  loss_report.unpaid_losses_net_of_salvage_subro, 
  loss_report.losses_incurred_direct, 
  loss_report.defense_direct_lae_paid, 
  loss_report.adjusting_direct_lae_paid, 
  loss_report.direct_lae_paid,
  loss_report.defense_direct_unpaid_lae_reported, 
  loss_report.adjusting_direct_unpaid_lae_reported, 
  loss_report.direct_unpaid_lae_reported,
  loss_report.defense_direct_lae_incurred, 
  loss_report.adjusting_direct_lae_incurred, 
  loss_report.lae_incurred_direct, 
  loss_report.claims_updated_at,
  loss_report.reserve_changes_id, 
  loss_report.reserve_changes_updated_at, 
  loss_report.claim_items_id, 
  loss_report.claim_items_updated_at, 
  loss_report.invoices_id, 
  loss_report.invoices_updated_at, 
  loss_report.transactions_id, 
  loss_report.transactions_updated_at, 
  loss_report.policies_id, 
  loss_report.policies_updated_at, 
  loss_report.quotes_id, 
  loss_report.quotes_updated_at, 
  loss_report.locations_id, 
  loss_report.locations_updated_at, 
  loss_report.address_id, 
  loss_report.address_updated_at, 
  loss_report.users_id, 
  loss_report.users_updated_at, 
  loss_report.causes_id, 
  loss_report.causes_updated_at, 
  loss_report.adjuster_fees_id, 
  loss_report.adjuster_fees_updated_at, 
  loss_report.feature_subrogation_logs_id, 
  loss_report.feature_subrogation_logs_updated_at, 
  loss_report.feature_subrogations_id, 
  loss_report.feature_subrogations_updated_at, 
  loss_report.salvage_changes_id, 
  loss_report.salvage_changes_updated_at, 
  loss_report.section AS section, 
  CURRENT_TIMESTAMP::timestamp_ntz AS created_at, 
  'update' AS record_type, 
  object_construct('reason', 'fix duplicated record originaly created on \'2022-08-16\', using additional negation recrods', 'type', 'bug_fix') AS comments
FROM loss_report
where 
       claim_internal_id = 954
        and salvage_changes_id = 1142
    and section = 'salvage'
        and unpaid_losses_net_of_salvage_subro in (0,-579);
;
 
  
 
