--public.dates

WITH dim_date_help AS
(SELECT CAST('1990-01-01' AS DATE) date_date
 UNION ALL
 SELECT DATEADD(DAY,1,dim_date_help.date_date) date_date 
 FROM dim_date_help WHERE date_date <'2099-12-31'
),

dim_date_cte AS(
select date_date
FROM dim_date_help
),

policy_versions_cte AS (
select 
    a.policy_version_id,
    a.premium_breakdown,
    TRY_PARSE_JSON(premium_breakdown) AS premium_breakdown_json, 
    premium_breakdown_json:endorsements AS endorsements
from monolith.policy_version_premiums a
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
    b.policy_id,
    policy_version_id,   
    b.start_date,
    b.end_date,
    endorsement_value AS annual_sum,--    calc_index,
    ROW_NUMBER() OVER (PARTITION BY policy_version_id, endorsement_type ORDER BY calc_index DESC) AS row_number_of_calc_index,
    endorsement_type
from policy_endorsements_cte a join monolith.policy_versions b on a.policy_version_id=b.id-- ?join monolith.policies c on b.policy_id=c.id
),

dates_cte AS(
SELECT pv.policy_id, 
    policy_version_id, 
    annual_sum,
    pv.start_date,
    convert_timezone('UTC', t.timezone, pv.start_date) AS start_date_local,
    --coalesce(pv.end_date, '2099-01-01') AS end_date,
    coalesce(pv.end_date, CURRENT_DATE()) AS end_date,    
    pv.end_date AS end_date_original,
    convert_timezone('UTC', t.timezone, coalesce(pv.end_date, CURRENT_DATE())) AS end_date_local,    
    pv.state,
    endorsement_type
FROM calc_index_cte bc join monolith.policy_versions pv on bc.policy_version_id = pv.id
    join timezones t on pv.state=t.state_code
WHERE row_number_of_calc_index = 1
    
),

date_diff_cte AS (
select 
    policy_id, 
    policy_version_id, 
    ROW_NUMBER() OVER (PARTITION BY policy_id, DATE(start_date_local), endorsement_type ORDER BY policy_version_id DESC) AS Last_version_a_day,
    ROW_NUMBER() OVER (PARTITION BY policy_id ORDER by policy_version_id DESC) AS last_version_per_policy,
    annual_sum, 
    endorsement_type,
    start_date_local as start_datetime,
    DATE(start_date_local) as start_date,
    end_date_local as end_datetime, 
    DATE(end_date_local) AS end_date,
    datediff(day,start_date_local, end_date_local) version_num_of_days,
    state
from dates_cte
),
    
one_version_per_day AS(
select policy_id, 
       policy_version_id,
       --Last_version_a_day,
       last_version_per_policy,
       annual_sum,
       endorsement_type,
       start_date,
       end_date,
       version_num_of_days,
       state   
from  date_diff_cte
where Last_version_a_day = 1
),

num_of_days_last_version_plus_one_join_policies AS (
select policy_id, 
       p.encrypted_id,
       policy_version_id,
       IFF(last_version_per_policy = 1, version_num_of_days + 1, version_num_of_days) AS num_of_days_plus_one_day,
       annual_sum,
       endorsement_type,
       start_date,
       end_date,
       a.state,
       years_insured,
       p.status--not the right one
FROM   one_version_per_day a join monolith.policies p on a.policy_id=p.id   
),

flatten_sum_for_days AS(
select policy_id, 
       status,
       encrypted_id,
       policy_version_id,
       annual_sum,
       start_date,
       end_date,
       state,
       years_insured,--not this
       t.date_date,
       annual_sum / DATEDIFF(day, start_date, end_date) as daily_sum
from num_of_days_last_version_plus_one_join_policies l join dim_date_cte t 
     ON l.start_date<=t.date_date AND t.date_date<end_date
--WHERE policy_id=5227949-- and policy_version_id<>25695886
)
 
select *
from flatten_sum_for_days
where status = 'canceled' --policy_id = 5227949
--group by status
order by policy_id, policy_version_id, date_date
;

select b.*
from monolith.policies a join monolith.policy_versions b on a.id = b.policy_id
where  a.id = 2339331
;
