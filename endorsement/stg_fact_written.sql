create or replace TABLE LEMONADE_DEVELOPMENT.BI_DEVELOPMENT.stg_endorsement_fact_written (
	ROW_ID NUMBER(38,0) autoincrement,
	policy_ID NUMBER(38,0),
	encrypted_policy_ID VARCHAR(100),
    policy_version_ID VARCHAR(100),
    endorsement_type VARCHAR(100),
    version_effective_date_from TIMESTAMP_NTZ(9),
    version_effective_date_to TIMESTAMP_NTZ(9),
    currency VARCHAR(10),
    value NUMBER(38,0),
    annual_premium NUMBER(38,0),
    version_num_of_days NUMBER(38,0), 
    policy_lifetime_in_days NUMBER(38,0),
    policy_effective_date TIMESTAMP_NTZ(9),
    policy_renewal_date TIMESTAMP_NTZ(9),
    endorsment_premium NUMBER(38,0),
    ETL_version_from TIMESTAMP_NTZ(9),
    ETL_version_to TIMESTAMP_NTZ(9)
);

insert into LEMONADE_DEVELOPMENT.BI_DEVELOPMENT.stg_endorsement_fact_written
(
POLICY_ID, ENCRYPTED_POLICY_ID, POLICY_VERSION_ID, ENDORSEMENT_TYPE, VERSION_EFFECTIVE_DATE_FROM, VERSION_EFFECTIVE_DATE_TO, CURRENCY, VALUE, ANNUAL_PREMIUM, VERSION_NUM_OF_DAYS, POLICY_LIFETIME_IN_DAYS, POLICY_EFFECTIVE_DATE, POLICY_RENEWAL_DATE, ENDORSMENT_PREMIUM, ETL_VERSION_FROM, ETL_VERSION_TO)

WITH policy_versions_cte AS (
select 
    pvp.policy_version_id,
    pvp.premium_breakdown,
    TRY_PARSE_JSON(premium_breakdown) AS premium_breakdown_json, 
    premium_breakdown_json:endorsements AS endorsements
from monolith.policy_version_premiums pvp
),

policy_endorsements_cte as(
select 
    policy_version_id,   
    value:type::string AS endorsement_type,
    value:value AS endorsement_value,
    value:calc_index AS calc_index
    from policy_versions_cte AS base,
        LATERAL FLATTEN(input => base.endorsements) AS endorsements
    --WHERE endorsement_type = 'lemonade_earthquake'
),

policy_events AS(
select policy_id, effective_date AS event_date, 'update' AS event_type
from public.policy_premium_activities a 
where activity='policy_update'
union
select policy_id, effective_date AS event_date, 'cancel' AS event_type
from public.policy_premium_activities a 
where activity='policy_cancelation'
union
select policy_id, effective_date AS event_date, 'reinstate' AS event_type
from public.policy_premium_activities a 
where activity='policy_reinstate'
),

calc_index_cte AS(
select
    pv.policy_id,
    policy_version_id,   
    pv.start_date,
    LAST_DAY(pv.end_date) AS end_of_month_date,
    pv.end_date AS end_date,
    convert_timezone('UTC', t.timezone, pv.start_date) AS start_date_local,
    endorsement_value as annual_premium,
    ROW_NUMBER() OVER (PARTITION BY policy_version_id, endorsement_type ORDER BY calc_index DESC) AS row_number_of_calc_index,
    endorsement_type
from policy_endorsements_cte a join monolith.policy_versions pv on a.policy_version_id=pv.id
     join timezones t on pv.state=t.state_code
),

date_diff_cte AS (
select 
    p.encrypted_id AS encrypted_policy_ID,
    policy_id, 
    policy_version_id, 
    ROW_NUMBER() OVER (PARTITION BY policy_id, DATE(start_date_local), endorsement_type ORDER BY policy_version_id DESC) AS Last_version_a_day,
    c.annual_premium, 
    endorsement_type,
    start_date AS version_effective_date_from,
    coalesce(end_date, p.renewal_date) AS version_effective_date_to,
    p.effective_date AS policy_effective_date,
    p.renewal_date AS policy_renewal_date
from calc_index_cte c JOIN monolith.policies p on c.policy_id=p.id
     WHERE row_number_of_calc_index = 1
),

one_version_per_day AS(
select DENSE_RANK() OVER (PARTITION BY policy_id ORDER by policy_version_id) AS number_of_version_per_policy,    
       encrypted_policy_ID,
       policy_id, 
       policy_version_id,
       annual_premium,
       endorsement_type,
       version_effective_date_from,
       version_effective_date_to, 
       policy_effective_date,
       policy_renewal_date,
       datediff(day,policy_effective_date, policy_renewal_date) policy_lifetime_in_days,
       datediff(day,version_effective_date_from, version_effective_date_to) version_num_of_days
from  date_diff_cte
where Last_version_a_day = 1
),

cte_first_version AS(
select 
       CASE WHEN number_of_version_per_policy = 1 THEN annual_premium 
            ELSE 0 end as value,
       annual_premium,
       encrypted_policy_ID,
       policy_id, 
       policy_version_id,       
       endorsement_type,
       version_effective_date_from,
       version_effective_date_to,
       policy_effective_date,
       policy_renewal_date,
       policy_lifetime_in_days,
       version_num_of_days,
version_num_of_days/policy_lifetime_in_days*annual_premium AS endorsment_premium
       --value / DATEDIFF(day, effective_date_from, effective_date_to) * version_num_of_days as sum_for_effective_days       
    from one_version_per_day
)

select a.policy_id, encrypted_policy_ID, policy_version_id, endorsement_type, version_effective_date_from, version_effective_date_to, 'USD' AS CURRENCY, 
VALUE, annual_premium, version_num_of_days, 
 policy_lifetime_in_days,
policy_effective_date,
policy_renewal_date,
endorsment_premium,
CURRENT_DATE(), 
NULL
--,pe.event_type, pe.event_date
from cte_first_version a-- join policy_events pe on a.policy_id=pe.policy_id
;
