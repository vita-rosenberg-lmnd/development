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
    --b.policy_id AS policy_id,
    a.policy_version_id,
    a.premium_breakdown,
    TRY_PARSE_JSON(premium_breakdown) AS premium_breakdown_json, 
    premium_breakdown_json:endorsements AS endorsements
from monolith.policy_version_premiums a --join monolith.policy_versions b on a.policy_version_id=b.id join monolith.policies c on b.policy_id=c.id
    ),
    
policy_endorsements_cte as(
select --policy_id,
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
    policy_version_id,
    endorsement_value,--    calc_index,
    ROW_NUMBER() OVER (PARTITION BY policy_version_id, endorsement_type ORDER BY calc_index DESC) AS row_number_of_calc_index,
    endorsement_type
from policy_endorsements_cte 
),

base_sum_cte AS(
select policy_version_id,
       endorsement_value AS annual_sum,--calc_index,
       endorsement_type
from calc_index_cte
where row_number_of_calc_index = 1
),

dates_cte AS(
SELECT pv.policy_id, 
    policy_version_id, 
    extract(year from pv.start_date) AS year,
    IFF(year%4 = 0 , 366, 365) AS num_of_days_in_year,--change effective date plus year 
    annual_sum/num_of_days_in_year as daily_sum, -- divide by 366 if 
    annual_sum, -- daily sum should be multiplied by num of days in a month to find earned
    endorsement_type,
    pv.start_date,
    convert_timezone('UTC', t.timezone, pv.start_date) AS start_date_local,
    --coalesce(pv.end_date, '2099-01-01') AS end_date,
    --coalesce(pv.end_date, CURRENT_DATE()) AS end_date, 
    convert_timezone('UTC', t.timezone, coalesce(pv.end_date, CURRENT_DATE())) AS end_date_local,    
    pv.state    
FROM base_sum_cte bc join monolith.policy_versions pv on bc.policy_version_id = pv.id
    join timezones t on pv.state=t.state_code
)
--select * from dates_cte
--where policy_version_id=25695886
--;

,

date_diff_cte AS (
select 
    policy_id, 
    policy_version_id, 
    ROW_NUMBER() OVER (PARTITION BY policy_id, DATE(start_date_local), endorsement_type ORDER BY policy_version_id DESC) AS Last_version_a_day,
    ROW_NUMBER() OVER (PARTITION BY policy_id ORDER by policy_version_id DESC) AS last_version_per_policy,
    daily_sum, 
    num_of_days_in_year,
    annual_sum, 
    endorsement_type,
    start_date_local as start_datetime,
    DATE(start_date_local) as start_date,
    end_date_local as end_datetime, 
    DATE(end_date_local) AS end_date,
    datediff(day,start_date_local, end_date_local) version_num_of_days,
    version_num_of_days * daily_sum AS amount_per_active_days,
    state
from dates_cte
)

--select * from date_diff_cte
----where Last_version_a_day<>1 and last_version_per_policy<>1
--where policy_id=3185651
--;
,
    
one_version_per_day AS(
select policy_id, 
       policy_version_id,
       --Last_version_a_day,
       last_version_per_policy,
       daily_sum, 
       --year,
       num_of_days_in_year,
       annual_sum,
       endorsement_type,
       start_date,
       end_date,
       version_num_of_days,
       amount_per_active_days,
       state   
from  date_diff_cte
where Last_version_a_day = 1
)

select * from one_version_per_day
where policy_id=3185651
;
,

num_of_days_last_version_plus_one AS (
select policy_id, 
       p.encrypted_id,
       policy_version_id,
       IFF(last_version_per_policy = 1, version_num_of_days + 1, version_num_of_days) AS num_of_days_plus_one_day,
       daily_sum, 
       num_of_days_plus_one_day * daily_sum as amount_per_active_days_plus_one, 
       --year,
       num_of_days_in_year,
       annual_sum,
       endorsement_type,
       start_date,
       start_date_UTC,
       start_date_time_UTC,
       LAST_DAY(DATE_TRUNC('month', start_date_time_UTC)::date) AS start_month_UTC,
       end_date,
       end_date_UTC,
       end_date_time_UTC,
       LAST_DAY(DATE_TRUNC('month', end_date_time_UTC)::date) AS end_month_UTC,
       num_of_days,
       num_of_days_UTC,
       amount_per_active_days,
       a.state,
       years_insured  
FROM   one_version_per_day a join monolith.policies b on a.policy_id=b.id   
),

flatten_sum_for_days AS(
select policy_id, 
       policy_version_id,
       --num_of_days_plus_one_day,
       --num_of_days_plus_one_day * daily_sum as amount_per_active_days_plus_one,
       daily_sum, 
       --year,
       num_of_days_in_year,
       annual_sum,
       start_date,
       start_date_UTC,
       start_date_time_UTC,
       start_month_UTC,
       end_date,
       end_date_UTC,
       end_date_time_UTC,
       end_month_UTC,
       num_of_days,
       num_of_days_UTC,
       amount_per_active_days,
       state,
       years_insured,--not this
       t.date_date
from num_of_days_last_version_plus_one l join dim_date_cte t 
     ON l.start_date_UTC<=t.date_date AND t.date_date<end_date_UTC
WHERE policy_id=5227949-- and policy_version_id<>25695886
)

create table fact_endorsement as
select * from flatten_sum_for_days
;

select to_varchar('2013-04-05'::date, 'yyyy - mm')
