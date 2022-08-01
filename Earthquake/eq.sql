with policy_versions_cte AS (
select 
    --b.policy_id AS policy_id,
    a.policy_version_id,
    a.premium_breakdown,
    TRY_PARSE_JSON(premium_breakdown) AS premium_breakdown_json, 
    premium_breakdown_json:endorsements AS endorsements
from monolith.policy_version_premiums a --join monolith.policy_versions b on a.policy_version_id=b.id join monolith.policies c on b.policy_id=c.id
    )
,
policy_endorsements_cte as(
select --policy_id,
    policy_version_id,   
    value:type::string AS endorsement_type,
    value:value AS endorsement_value,
    value:calc_index AS calc_index
    from policy_versions_cte AS base,
        LATERAL FLATTEN(input => base.endorsements) AS endorsements
    WHERE endorsement_type = 'lemonade_earthquake'
),

--select count(*)
base_sum_cte AS(
select --policy_id,
policy_version_id,
--sum(endorsement_value) AS annual_sum,
endorsement_value,    
ROW_NUMBER() OVER (PARTITION BY policy_version_id ORDER BY calc_index DESC) AS rn,
calc_index
from policy_endorsements_cte 
--group by --policy_id,
--policy_version_id--, calc_index
)
--,
--,
--select policy_version_id, sum(endorsement_value) AS annual_sum
--from base_sum_cte
--where rn = 1
--group by policy_version_id


select * -- 189,671
from base_sum_cte
where rn = 1
--sss AS (select endorsement_value,
--ROW_NUMBER() OVER (PARTITION BY policy_version_id ORDER BY endorsement_value) AS rn, policy_version_id
--from base_sum_cte)

select policy_version_id,
endorsement_value,
--annual_sum,
endorsement_value, 
rn,
calc_index
FROM base_sum_cte
where policy_version_id = 22068486
--where rn<>1
--limit 1000


select count(*)--189670 with group by
--no group by 190,317
from base_sum_cte

dates_cte AS(
SELECT pv.policy_id, 
policy_version_id, 
annual_sum/365 as daily_sum, 
annual_sum,
pv.start_date,
convert_timezone('UTC', t.timezone, pv.start_date) AS start_date_UTC,
coalesce(pv.end_date, current_timestamp()) AS end_date,
convert_timezone('UTC', t.timezone, coalesce(pv.end_date, current_timestamp())) AS end_date_UTC,    
pv.state    
FROM base_sum_cte bc join monolith.policy_versions pv on bc.policy_version_id = pv.id
    join timezones t on pv.state=t.state_code
--and bc.policy_version_id=pv.id
),
    
date_diff_cte AS (
select 
    policy_id, 
    policy_version_id,daily_sum, 
    annual_sum, 
    start_date_UTC,
    start_date, 
    end_date_UTC,
    end_date, 
    datediff(day,start_date_UTC, end_date_UTC) num_of_days_UTC,
    datediff(day,start_date, end_date) num_of_days, 
    state
from dates_cte
),
 
flat_versions AS (select policy_id, 
       policy_version_id,
       daily_sum,
       annual_sum,
       start_date,
       start_date_UTC AS start_date_time_UTC,
       DATE(start_date_UTC) AS start_date_UTC,
       end_date,
       end_date_UTC AS end_date_time_UTC,
       DATE(end_date_UTC) AS end_date_UTC,
       num_of_days,
       num_of_days_UTC,
       num_of_days*daily_sum as amount_per_active_days, 
       state
from date_diff_cte dd-- join timezones t on dd.state=t.state_code
--where policy_id = 5227949
--WHERE num_of_days_UTC<>num_of_days
),

--select * from flat_versions
--where policy_id = 5227949

a_few_versions_a_day AS(
select policy_id, 
       policy_version_id,
       ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY policy_version_id DESC) AS Last_version_a_day,
       daily_sum,
       annual_sum,
       start_date,
       start_date_UTC,
       start_date_time_UTC,
       end_date,
       end_date_UTC,
       end_date_time_UTC,
       num_of_days,
       num_of_days_UTC,
       amount_per_active_days,
       state
from flat_versions  
--group by policy_id
order by policy_id,
start_date_UTC
--where policy_id = 5227949
    )
select policy_id, 
       policy_version_id,
       Last_version_a_day,
       daily_sum,
       annual_sum,
       start_date,
       start_date_UTC,
       start_date_time_UTC,
       end_date,
       end_date_UTC,
       end_date_time_UTC,
       num_of_days,
       num_of_days_UTC,
       amount_per_active_days,
       state   
from  a_few_versions_a_day
--where Last_version_a_day=1
where policy_id=2523935
--where start_date_UTC=end_date_UTC



select * from timezones;


select TO_VARCHAR(CONVERT_TIMEZONE('UTC' ,events.timezone , original_policies.created_at)::date, 'YYYYMMDD')::int,


SELECT * FROM monolith.policy_versions limit 100

select * from monolith.policies
limit 100

select * from monolith.policy_version_premiums
 

select * from public.policy_premium_activities




with policy_versions_cte AS (
select 
    b.policy_id AS policy_id,
    a.policy_version_id,
    TRY_PARSE_JSON(premium_breakdown) AS premium_breakdown_json, 
    premium_breakdown_json:endorsements AS endorsements
from monolith.policy_version_premiums a join monolith.policy_versions b on a.policy_version_id=b.id join monolith.policies c on b.policy_id=c.id
    )
select * from policy_versions_cte
where policy_id = 5227949

select convert_timezone('America/Los_Angeles', 'America/New_York', '2019-01-01 14:00:00'::timestamp_ntz) as conv;

select 
start_date, 
end_date, 
convert_timezone('UTC', t.timezone, start_date) AS start_date_UTC,
convert_timezone(t.timezone, end_date) AS end_date_UTC,
pv.state,*
from monolith.policy_versions pv join timezones t on pv.state=t.state_code
where policy_id = 5227949


select 
start_date, 
end_date,
*
from monolith.policy_versions
where policy_id = 5227949

select * from monolith.policy_version_premiums
where policy_version_id 
IN
(
27233553
,22816797
,20405928
,21910478
,22362905
,24519988
,21237983
,22337205)
