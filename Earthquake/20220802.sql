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

calc_index_cte AS(
select
    policy_version_id,
    endorsement_value,    
    ROW_NUMBER() OVER (PARTITION BY policy_version_id ORDER BY calc_index DESC) AS row_number_of_calc_index
from policy_endorsements_cte 
),

base_sum_cte AS(
select policy_version_id,
       endorsement_value AS annual_sum
from calc_index_cte
where row_number_of_calc_index = 1
),

dates_cte AS(
SELECT pv.policy_id, 
policy_version_id, 
annual_sum/365 as daily_sum, -- divide by 366 if 
annual_sum, -- daily sum should be multiplied by num of days in a month to find earned
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
    policy_version_id, 
    daily_sum, 
    annual_sum, 
    start_date_UTC,
    start_date, 
    end_date_UTC,
    end_date, 
    datediff(day,start_date_UTC, end_date_UTC) num_of_days_UTC, -- if last_policy_version_day = last_day_of_policy than plus 1
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

a_few_versions_a_day AS(
select policy_id, 
       policy_version_id,
       ROW_NUMBER() OVER (PARTITION BY policy_id, start_date_UTC ORDER BY policy_version_id DESC) AS Last_version_a_day,
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
    )
--select policy_id, count(policy_id) from (
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
where Last_version_a_day = 1
--order by policy_id
    --)
    --group by policy_id
    --having count(policy_id) > 1
AND policy_id=2523935
--AND policy_id=5128305
ORDER BY policy_version_id
--where start_date_UTC=end_date_UTC
;
143
14
4

select datediff(day,'2022-02-22 00:01:00.000','2022-03-08 04:10:51.000')--14
select datediff(day,'2022-03-08 04:10:52.000','2022-03-12 14:05:20.000')--4
select datediff(day,'2022-03-12 14:05:21.000','2022-04-02 04:43:06.902')--21 -> 22
