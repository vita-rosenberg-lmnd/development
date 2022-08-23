set start_day_of_prev_month = '2022-01-01' ;
set last_day_of_prev_month = '2022-12-01';

select * from table(PUBLIC.GET_US_HOME_LOSS_REPORT(  
          $start_day_of_prev_month, 
          $last_day_of_prev_month
        ))
        where general_claim = 'LC9A9431B2'
        --where FORM is not null
order by general_claim        
;

select * from  TABLE(
        GET_EU_HOME_LOSS_REPORT(
          $start_day_of_prev_month, 
          $last_day_of_prev_month
        )
      )
              where general_claim = 'LC9A9431B2'
        --where FORM is not null
order by general_claim    
;
--INSERT INTO sox_finance.stg_workday_loss_reports
    WITH helper AS (
      SELECT
        general_claim,
        claim_internal_id,
        country,
        status,
        null AS feature_id,
        null AS feature_group,
        feature_type,
        claim_payor,
        time_of_loss,
        report_time,
        transaction_type,
        accounting_date,
        cohort_id,
        cohort,
        policy,
        policy_internal_id,
        form,
        line_of_business,
        state,
        effective_date,
        direct_losses_paid,
        salvage_received_direct,
        subro_received_direct,
        unpaid_losses_reported,
        salvage_anticipated_direct,
        subro_anticipated_direct,
        losses_incurred_direct,
        defense_direct_lae_paid,
        adjusting_direct_lae_paid,
        direct_lae_paid,
        losses_paid_direct_net_of_salvage_subro,
        defense_direct_unpaid_lae_reported,
        adjusting_direct_unpaid_lae_reported,
        direct_unpaid_lae_reported,
        unpaid_losses_net_of_salvage_subro,
        defense_direct_lae_incurred,
        adjusting_direct_lae_incurred,
        lae_incurred_direct,
        created_at,
        catastrophe_number,
        reporting_code,
        state_claim_occurred,
        country_claim_occurred,
        expense_title,
        null AS vendor_engaged
      FROM TABLE(
        get_car_loss_report(  
          $start_day_of_prev_month, 
          $last_day_of_prev_month
        )
      )
    
      UNION ALL
        
      SELECT
        general_claim,
        claim_internal_id,
        country,
        status,
        feature_id AS feature_id,
        null AS feature_group,
        feature_type,
        claim_payor,
        time_of_loss,
        report_time,
        transaction_type,
        accounting_date,
        cohort_id,
        cohort,
        policy,
        policy_internal_id,
        form,
        line_of_business,
        state,
        effective_date,
        direct_losses_paid,
        null AS salvage_received_direct,
        salvage_and_subro_received_direct AS subro_received_direct,
        unpaid_losses_reported,
        null AS salvage_anticipated_direct,
        salvage_and_subro_anticipated_direct AS subro_anticipated_direct,
        losses_incurred_direct,
        defense_direct_lae_paid,
        adjusting_direct_lae_paid,
        direct_lae_paid,
        null AS losses_paid_direct_net_of_salvage_subro,
        defense_direct_unpaid_lae_reported,
        adjusting_direct_unpaid_lae_reported,
        direct_unpaid_lae_reported,
        null AS unpaid_losses_net_of_salvage_subro,
        defense_direct_lae_incurred,
        adjusting_direct_lae_incurred,
        lae_incurred_direct,
        created_at,
        null AS catastrophe_number,
        null AS reporting_code,
        null AS state_claim_occurred,
        null AS country_claim_occurred,
        null expense_title,
        null AS vendor_engaged
      FROM TABLE(
        GET_US_HOME_LOSS_REPORT(
          $start_day_of_prev_month, 
          $last_day_of_prev_month
        )
      )
    
      UNION ALL
        
      SELECT
        general_claim,
        claim_internal_id,
        country,
        status,
        feature_id AS feature_id,
        null AS feature_group,
        feature_type,
        claim_payor,
        time_of_loss,
        report_time,
        transaction_type,
        accounting_date,
        cohort_id,
        cohort,
        policy,
        policy_internal_id,
        form,
        line_of_business,
        state,
        effective_date,
        direct_losses_paid,
        null AS salvage_received_direct,
        salvage_and_subro_received_direct AS subro_received_direct,
        unpaid_losses_reported,
        null AS salvage_anticipated_direct,
        salvage_and_subro_anticipated_direct AS subro_anticipated_direct,
        losses_incurred_direct,
        defense_direct_lae_paid,
        adjusting_direct_lae_paid,
        direct_lae_paid,
        null AS losses_paid_direct_net_of_salvage_subro,
        defense_direct_unpaid_lae_reported,
        adjusting_direct_unpaid_lae_reported,
        direct_unpaid_lae_reported,
        null AS unpaid_losses_net_of_salvage_subro,
        defense_direct_lae_incurred,
        adjusting_direct_lae_incurred,
        lae_incurred_direct,
        created_at,
        null AS catastrophe_number,
        null AS reporting_code,
        null AS state_claim_occurred,
        null AS country_claim_occurred,
        null expense_title,
        null AS vendor_engaged
      FROM TABLE(
        GET_EU_HOME_LOSS_REPORT(
          $start_day_of_prev_month, 
          $last_day_of_prev_month
        )
      )
        
      UNION ALL
        
      SELECT
        general_claim,
        null AS claim_internal_id,
        'us' AS country,
        null AS status,
        null AS feature_id,
        null AS feature_group,
        feature AS feature_type,
        claim_payor,
        time_of_loss,
        report_time,
        transaction_type,
        accounting_date,
        cohort_id,
        cohort,
        policy,
        null AS policy_internal_id,
        null AS form,
        null AS line_of_business,
        state,
        effective_date,
        direct_losses_paid,
        null AS salvage_received_direct,
        salvage_and_subro_received_direct as subro_received_direct,
        unpaid_losses_reported,
        null AS salvage_anticipated_direct,
        salvage_and_subro_anticipated_direct as subro_anticipated_direct,
        losses_incurred_direct,
        defence_direct_lae_paid as defense_direct_lae_paid,
        adjusting_direct_lae_paid,
        direct_lae_paid,
        null AS losses_paid_direct_net_of_salvage_subro,
        defence_direct_unpaid_lae_reported as defense_direct_unpaid_lae_reported,
        adjusting_direct_unpaid_lae_reported,
        direct_unpaid_lae_reported,
        null AS unpaid_losses_net_of_salvage_subro,
        defence_direct_lae_incurred as defense_direct_lae_incurred,
        adjusting_direct_lae_incurred,
        lae_incurred_direct,
        created_at,
        null AS catastrophe_number,
        null AS reporting_code,
        null AS state_claim_occurred,
        null AS country_claim_occurred,
        null expense_title,
        null AS vendor_engaged
      FROM TABLE(
        GET_PET_LOSS_REPORT(
          $start_day_of_prev_month, 
          $last_day_of_prev_month
        )
      )
    )
    
    SELECT
      $last_day_of_prev_month::date AS month,
      --last_day_of_prev_month
      LAST_DAY(DATE_TRUNC('quarter', MONTH)::DATE + INTERVAL '2 months') AS quarter,
      general_claim,
      claim_internal_id,
      country,
      status,
      feature_id,
      feature_group,
      feature_type,
      claim_payor,
      SUBSTRING(time_of_loss, 0, 19) AS time_of_loss,
      SUBSTRING(report_time, 0, 19) AS report_time,
      transaction_type,
      accounting_date,
      cohort_id,
      cohort,
      policy,
      policy_internal_id,
      form,
      line_of_business,
      state,
      effective_date,
      direct_losses_paid,
      salvage_received_direct,
      subro_received_direct,
      unpaid_losses_reported,
      salvage_anticipated_direct,
      subro_anticipated_direct,
      losses_incurred_direct,
      defense_direct_lae_paid,
      adjusting_direct_lae_paid,
      direct_lae_paid,
      losses_paid_direct_net_of_salvage_subro,
      defense_direct_unpaid_lae_reported,
      adjusting_direct_unpaid_lae_reported,
      direct_unpaid_lae_reported,
      unpaid_losses_net_of_salvage_subro,
      defense_direct_lae_incurred,
      adjusting_direct_lae_incurred,
      lae_incurred_direct,
      SUBSTRING(created_at, 0, 19) AS created_at,
      catastrophe_number,
      reporting_code,
      state_claim_occurred,
      country_claim_occurred,
      expense_title,
      vendor_engaged,
      null AS dummy_1,
      null AS dummy_2,
      null AS dummy_3,
      null AS dummy_4,
      null AS dummy_5,
      null AS dummy_6,
      null AS dummy_7,
      null AS dummy_8,
      null AS dummy_9,
      null AS dummy_10,
      null AS dummy_11,
      null AS dummy_12,
      null AS dummy_13,
      null AS dummy_14,
      null AS dummy_15,
      null AS dummy_16,
      null AS dummy_17,
      null AS dummy_18,
      null AS dummy_19,
      null AS dummy_20
    FROM helper
    where general_claim = 'LC9A9431B2'
    ;
    
    
  
