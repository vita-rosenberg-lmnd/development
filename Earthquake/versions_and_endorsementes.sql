with policy_versions_cte AS (
select 
    b.policy_id AS policy_id,
    policy_version_id,
    premium_breakdown,
    TRY_PARSE_JSON(premium_breakdown) AS premium_breakdown_json, 
    premium_breakdown_json:endorsements AS endorsements
from monolith.policy_version_premiums a join monolith.policy_versions b on a.policy_version_id=b.id join monolith.policies c on b.policy_id=c.id
    )
,
policy_endorsements_cte as(
select policy_id,
    policy_version_id,   
    value:type::string AS endorsement_type,
    value:value AS endorsement_value
    from policy_versions_cte AS base,
        LATERAL FLATTEN(input => base.endorsements) AS endorsements
    WHERE endorsement_type = 'lemonade_earthquake'
),

--select count(*)--181,881
base_sum_cte AS(
select policy_id,
policy_version_id,
sum(endorsement_value) AS annual_sum
from policy_endorsements_cte 
group by policy_id,
policy_version_id
),

dates_cte AS(
SELECT bc.policy_id, 
policy_version_id, 
annual_sum/365 as daily_sum, 
annual_sum,
pv.start_date, 
pv.end_date
FROM base_sum_cte bc join monolith.policy_versions pv on bc.policy_id = pv.policy_id
and bc.policy_version_id=pv.id
    ),
    
date_diff_cte AS (
select policy_id, 
policy_version_id,daily_sum, 
annual_sum, start_date, end_date, datediff(day,start_date,end_date) num_of_days
from dates_cte
 )
 
select  policy_id, 
policy_version_id,daily_sum, 
annual_sum, start_date, end_date, num_of_days,
num_of_days*daily_sum as amount_per_active_days
from date_diff_cte
where policy_id = 5227949 
