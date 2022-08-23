      MERGE INTO car_finance.loss_report_raw AS target
        USING (
            WITH last_synced AS (
                SELECT MAX(accounting_date) AS max_accounting_date
                FROM car_finance.loss_report_raw
                WHERE 
                  accounting_date::date < created_at::date
            ),
            
            final_salvage_events AS (
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
            
            base_loss_report AS (
                SELECT
                  c.status AS status,
                  'CAR' AS line_of_business,
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
                'CAR' AS line_of_business,
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
                'CAR' AS line_of_business,
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
                'CAR' AS line_of_business,
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
              *,
              CURRENT_TIMESTAMP::timestamp_ntz AS created_at,
              'new' AS record_type,
              NULL AS comments 
            FROM base_loss_report
            WHERE 
              accounting_date > (SELECT max_accounting_date FROM last_synced)
              AND accounting_date < CURRENT_DATE
            --ORDER BY CAST (claim_internal_id AS INTEGER), accounting_date ASC
            ) AS source
                ON
                    source.section = target.section
                    AND source.claim_internal_id = target.claim_internal_id
                    AND source.accounting_date = target.accounting_date
                    AND (
                        (
                         source.reserve_changes_id IS NOT NULL
                         AND source.section = 'reserve' 
                         AND source.reserve_changes_id = target.reserve_changes_id
                        )
                        OR (
                         source.claim_fee_changes_id IS NOT NULL
                         AND source.section = 'fees'
                         AND source.claim_fee_changes_id = target.claim_fee_changes_id
                        ) 
                        OR (
                         source.subrogation_changes_id IS NOT NULL
                         AND source.section = 'subro'
                         AND source.subrogation_changes_id = target.subrogation_changes_id
                        )
                        OR (
                        source.subrogation_changes_id IS NOT NULL
                        AND source.section = 'salvage'
                        AND source.subrogation_changes_id = target.subrogation_changes_id
                      ))
              WHEN NOT MATCHED THEN INSERT (
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
                  VALUES (
                   source.status
                  ,source.line_of_business
                  ,source.reporting_code
                  ,source.feature_type
                  ,source.general_claim
                  ,source.claim_internal_id
                  ,source.catastrophe_number
                  ,source.state_claim_occurred
                  ,source.country_claim_occurred
                  ,source.claim_payor
                  ,source.time_of_loss
                  ,source.report_time
                  ,source.transaction_type
                  ,source.accounting_date
                  ,source.cohort_id
                  ,source.cohort
                  ,source.policy
                  ,source.policy_internal_id
                  ,source.form
                  ,source.state
                  ,source.country
                  ,source.effective_date
                  ,source.expense_title
                  ,source.direct_losses_paid
                  ,source.salvage_received_direct
                  ,source.subro_received_direct
                  ,source.losses_paid_direct_net_of_salvage_subro
                  ,source.unpaid_losses_reported
                  ,source.salvage_anticipated_direct
                  ,source.subro_anticipated_direct
                  ,source.unpaid_losses_net_of_salvage_subro
                  ,source.losses_incurred_direct
                  ,source.defense_direct_lae_paid
                  ,source.adjusting_direct_lae_paid
                  ,source.direct_lae_paid
                  ,source.defense_direct_unpaid_lae_reported
                  ,source.adjusting_direct_unpaid_lae_reported
                  ,source.direct_unpaid_lae_reported
                  ,source.defense_direct_lae_incurred
                  ,source.adjusting_direct_lae_incurred
                  ,source.lae_incurred_direct
                  ,source.claims_updated_at
                  ,source.reserve_changes_id
                  ,source.reserve_changes_updated_at
                  ,source.claim_items_id
                  ,source.claim_items_updated_at
                  ,source.invoices_id
                  ,source.invoices_updated_at
                  ,source.transactions_id
                  ,source.transactions_updated_at
                  ,source.policies_id
                  ,source.policies_updated_at
                  ,source.quotes_id
                  ,source.quotes_updated_at
                  ,source.locations_id
                  ,source.locations_updated_at
                  ,source.address_id
                  ,source.address_updated_at
                  ,source.users_id
                  ,source.users_updated_at
                  ,source.causes_id
                  ,source.causes_updated_at
                  ,source.adjuster_fees_id
                  ,source.adjuster_fees_updated_at
                  ,source.feature_subrogation_logs_id
                  ,source.feature_subrogation_logs_updated_at
                  ,source.feature_subrogations_id
                  ,source.feature_subrogations_updated_at
                  ,source.salvage_changes_id
                  ,source.salvage_changes_updated_at
                  ,source.section
                  ,source.created_at
                  ,source.record_type
                  ,source.comments
                  )
                  
            ;
            
            
            
-- (
--             status, line_of_business, reporting_code, feature_type, general_claim, claim_internal_id,
--             catastrophe_number, state_claim_occurred, country_claim_occurred, claim_payor, time_of_loss,
--             report_time, transaction_type, accounting_date, cohort_id, cohort, policy, policy_internal_id,
--             form, state, country, effective_date, expense_title, direct_losses_paid, salvage_received_direct,
--             subro_received_direct, losses_paid_direct_net_of_salvage_subro, unpaid_losses_reported,
--             salvage_anticipated_direct, subro_anticipated_direct, unpaid_losses_net_of_salvage_subro, 
--             losses_incurred_direct, defense_direct_lae_paid, adjusting_direct_lae_paid, direct_lae_paid,
--             defense_direct_unpaid_lae_reported, adjusting_direct_unpaid_lae_reported, direct_unpaid_lae_reported,
--             defense_direct_lae_incurred, adjusting_direct_lae_incurred, lae_incurred_direct, claims_updated_at,
--             reserve_changes_id, reserve_changes_updated_at, claim_items_id, claim_items_updated_at, invoices_id, 
--             invoices_updated_at, transactions_id, transactions_updated_at, policies_id, policies_updated_at, 
--             quotes_id, quotes_updated_at, locations_id, locations_updated_at, address_id, address_updated_at, 
--             users_id, users_updated_at, causes_id, causes_updated_at, adjuster_fees_id, adjuster_fees_updated_at, 
--             feature_subrogation_logs_id, feature_subrogation_logs_updated_at, feature_subrogations_id, 
--             feature_subrogations_updated_at, salvage_changes_id, salvage_changes_updated_at, section, 
--             created_at, record_type, comments            
