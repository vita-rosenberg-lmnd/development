;
create or replace TABLE LEMONADE_DEVELOPMENT.BI_DEVELOPMENT.endorsement_fact_written (
	ROW_ID NUMBER(38,0) autoincrement,
	policy_ID NUMBER(38,0),
	encrypted_policy_ID VARCHAR(100),
    policy_version_ID VARCHAR(100),
    endorsement_type VARCHAR(100),
    effective_date_from TIMESTAMP_NTZ(9),
    effective_date_to TIMESTAMP_NTZ(9),
    currency VARCHAR(10),
    change_value NUMBER(38,0),
    value NUMBER(38,0),
    annual NUMBER(38,0),
    ETL_version_from TIMESTAMP_NTZ(9),
    ETL_version_to TIMESTAMP_NTZ(9)
);


insert into LEMONADE_DEVELOPMENT.BI_DEVELOPMENT.endorsement_fact_written
(
policy_id, encrypted_policy_ID, policy_version_id, endorsement_type, ETL_VERSION_FROM, ETL_VERSION_TO, effective_date_from, effective_date_to, currency, CHANGE_VALUE, VALUE, annual
)
;
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

calc_index_cte AS(
select
    pv.policy_id,
    policy_version_id,   
    pv.start_date,
    LAST_DAY(pv.end_date) AS end_of_month_date,
    pv.end_date AS end_date,
    convert_timezone('UTC', t.timezone, pv.start_date) AS start_date_local,
    endorsement_value as value,
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
    value, 
    endorsement_type,
    start_date AS effective_date_from,
    coalesce(end_date, p.renewal_date) AS effective_date_to
from calc_index_cte c JOIN monolith.policies p on c.policy_id=p.id
     WHERE row_number_of_calc_index = 1
),
    
one_version_per_day AS(
select DENSE_RANK() OVER (PARTITION BY policy_id ORDER by policy_version_id) AS number_of_version_per_policy,    
       encrypted_policy_ID,
       policy_id, 
       policy_version_id,
       value,
       value AS change_value,
       endorsement_type,
       effective_date_from,
       effective_date_to, 
       datediff(day,effective_date_from, effective_date_to) version_num_of_days
from  date_diff_cte
where Last_version_a_day = 1
)

select * from one_version_per_day
where version_num_of_days=0
;
,

cte_first_version AS(
select 
       CASE WHEN number_of_version_per_policy = 1 THEN value 
            ELSE 0 end as value,
       value as annual,
       encrypted_policy_ID,
       policy_id, 
       policy_version_id,
       change_value,
       endorsement_type,
       effective_date_from,
       effective_date_to,
       version_num_of_days,
       value / DATEDIFF(day, effective_date_from, effective_date_to) * version_num_of_days as sum_for_effective_days       
    from one_version_per_day
)

select policy_id, encrypted_policy_ID, policy_version_id, endorsement_type, 
CURRENT_DATE(), NULL, EFFECTIVE_DATE_FROM, EFFECTIVE_DATE_TO, 'USD', CHANGE_VALUE, VALUE, annual, version_num_of_days, sum_for_effective_days
from cte_first_version
