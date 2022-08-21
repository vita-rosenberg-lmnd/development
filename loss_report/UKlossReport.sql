CREATE OR REPLACE FUNCTION LEMONADE.PUBLIC.GET_UK_HOME_LOSS_REPORT("START_DATE" VARCHAR(16777216), "END_DATE" VARCHAR(16777216))
RETURNS TABLE ("GENERAL_CLAIM" VARCHAR(16777216), "CLAIM_INTERNAL_ID" VARCHAR(16777216), "COUNTRY" VARCHAR(16777216), "STATUS" VARCHAR(16777216), "FEATURE_ID" VARCHAR(16777216), "FEATURE_TYPE" VARCHAR(16777216), "CLAIM_PAYOR" VARCHAR(16777216), "TIME_OF_LOSS" TIMESTAMP_NTZ(9), "REPORT_TIME" TIMESTAMP_NTZ(9), "TRANSACTION_TYPE" VARCHAR(16777216), "ACCOUNTING_DATE" DATE, "COHORT_ID" VARCHAR(16777216), "COHORT" VARCHAR(16777216), "POLICY" VARCHAR(16777216), "POLICY_INTERNAL_ID" VARCHAR(16777216), "FORM" VARCHAR(16777216), "LINE_OF_BUSINESS" VARCHAR(16777216), "SUBROGATION" VARCHAR(16777216), "STATE" VARCHAR(16777216), "EFFECTIVE_DATE" DATE, "DIRECT_LOSSES_PAID" NUMBER(38,6), "SALVAGE_AND_SUBRO_RECEIVED_DIRECT" NUMBER(38,6), "UNPAID_LOSSES_REPORTED" NUMBER(38,6), "SALVAGE_AND_SUBRO_ANTICIPATED_DIRECT" NUMBER(38,6), "LOSSES_INCURRED_DIRECT" NUMBER(38,6), "DEFENSE_DIRECT_LAE_PAID" NUMBER(38,6), "ADJUSTING_DIRECT_LAE_PAID" NUMBER(38,6), "DIRECT_LAE_PAID" NUMBER(38,6), "DEFENSE_DIRECT_UNPAID_LAE_REPORTED" NUMBER(38,6), "ADJUSTING_DIRECT_UNPAID_LAE_REPORTED" NUMBER(38,6), "DIRECT_UNPAID_LAE_REPORTED" NUMBER(38,6), "DEFENSE_DIRECT_LAE_INCURRED" NUMBER(38,6), "ADJUSTING_DIRECT_LAE_INCURRED" NUMBER(38,6), "LAE_INCURRED_DIRECT" NUMBER(38,6), "CREATED_AT" TIMESTAMP_LTZ(9))
LANGUAGE SQL
COMMENT='Get home loss report for UK policies for specific dates period.'
AS '
    WITH loss_report_raw AS (
    SELECT *
              FROM finance.loss_report_raw
              WHERE
                accounting_date BETWEEN TO_DATE(start_date) AND TO_DATE(end_date)
    ),

    uk_claims AS (
      SELECT claim_internal_id
      FROM loss_report_raw
      GROUP BY claim_internal_id
      HAVING
        SUM(IFF(country IN (''UK''), 1, 0)) > 0
        AND SUM(IFF(status = ''canceled'', 1, 0)) = 0
    ),

    latest_loss_report_records AS (
      SELECT *
      FROM loss_report_raw
      QUALIFY ROW_NUMBER() OVER (
          PARTITION BY
              section,
              claim_internal_id,
              COALESCE(reserve_changes_id::string, claim_fee_changes_id::string, subrogation_changes_id::string, transactions_id::string)
          ORDER BY
              accounting_date DESC,
              created_at DESC,
              transactions_updated_at DESC
      ) = 1
    ),

    loss_report AS (
      SELECT
          raw_records.general_claim,
          raw_records.claim_internal_id,
          raw_records.country,
          IFF(
            LAST_VALUE(latest_records.status respect nulls) OVER (PARTITION BY raw_records.general_claim ORDER BY raw_records.created_at) = ''final_payment'',
            ''open'',
            LAST_VALUE(latest_records.status respect nulls) OVER (PARTITION BY raw_records.general_claim ORDER BY raw_records.created_at)
          ) AS status,
          latest_records.feature_id,
          latest_records.feature_type,
          raw_records.claim_payor,
          LAST_VALUE(latest_records.time_of_loss respect nulls) OVER (PARTITION BY raw_records.general_claim ORDER BY raw_records.created_at) AS time_of_loss,
          LAST_VALUE(latest_records.report_time respect nulls) OVER (PARTITION BY raw_records.general_claim ORDER BY raw_records.created_at) AS report_time,
          latest_records.transaction_type,
          raw_records.accounting_date,
          latest_records.cohort_id,
          latest_records.cohort,
          LAST_VALUE(latest_records.policy respect nulls) OVER (PARTITION BY raw_records.general_claim ORDER BY raw_records.created_at) AS policy,
          LAST_VALUE(latest_records.policy_internal_id respect nulls) OVER (PARTITION BY raw_records.general_claim ORDER BY raw_records.created_at) AS policy_internal_id,
          LAST_VALUE(latest_records.form respect nulls) OVER (PARTITION BY raw_records.general_claim ORDER BY raw_records.created_at) AS form,
          latest_records.line_of_business,
          raw_records.subrogation,
          LAST_VALUE(latest_records.state respect nulls) OVER (PARTITION BY raw_records.general_claim ORDER BY raw_records.created_at) AS state,
          LAST_VALUE(latest_records.effective_date respect nulls) OVER (PARTITION BY raw_records.general_claim ORDER BY raw_records.created_at) AS effective_date,
          raw_records.direct_losses_paid,
          raw_records.salvage_and_subro_received_direct,
          raw_records.unpaid_losses_reported,
          raw_records.salvage_and_subro_anticipated_direct,
          raw_records.losses_incurred_direct,
          raw_records.defense_direct_lae_paid,
          raw_records.adjusting_direct_lae_paid,
          raw_records.direct_lae_paid,
          raw_records.defense_direct_unpaid_lae_reported,
          raw_records.adjusting_direct_unpaid_lae_reported,
          raw_records.direct_unpaid_lae_reported,
          raw_records.defense_direct_lae_incurred,
          raw_records.adjusting_direct_lae_incurred,
          raw_records.lae_incurred_direct,
          raw_records.created_at
      FROM loss_report_raw AS raw_records
      JOIN uk_claims
        ON uk_claims.claim_internal_id = raw_records.claim_internal_id
      JOIN latest_loss_report_records AS latest_records
        ON latest_records.claim_internal_id = raw_records.claim_internal_id
          AND latest_records.section = raw_records.section
          AND (
            (
              latest_records.reserve_changes_id IS NOT NULL
              AND latest_records.section = ''reserve''
              AND latest_records.reserve_changes_id = raw_records.reserve_changes_id
            ) OR (
              latest_records.claim_fee_changes_id IS NOT NULL
              AND latest_records.section = ''fees''
              AND latest_records.claim_fee_changes_id = raw_records.claim_fee_changes_id
            ) OR (
              latest_records.subrogation_changes_id IS NOT NULL
              AND latest_records.section = ''salvage_subro''
              AND latest_records.subrogation_changes_id = raw_records.subrogation_changes_id
            ) OR (
              latest_records.transactions_id IS NOT NULL
              AND latest_records.section = ''claims_refund''
              AND latest_records.transactions_id = raw_records.transactions_id
            )
          )
    )

    SELECT *
    FROM loss_report
  ';
