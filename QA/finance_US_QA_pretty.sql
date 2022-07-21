WITH temp_premium_report_us AS(
    select * 
    FROM FINANCE.PREMIUM_REPORT_US
),

--Not canclled with written or earned = 0
not_cancelled AS(
    SELECT encrypted_id
    from monolith.policies
    where canceled_date IS NULL
    and effective_date<current_date()
),

not_cancelled_monthly_written_premium AS(
select sum(monthly_written_premium) AS sum_of_type, encrypted_id, 'not_cancelled_monthly_written_premium' AS errType
from temp_premium_report_us
where encrypted_id in(select encrypted_id from not_cancelled)
group by encrypted_id
having (-0.01) < SUM(monthly_written_premium) and SUM(monthly_written_premium) < (0.01)
),
not_cancelled_monthly_earned_premium AS(
select sum(monthly_earned_premium) AS sum_of_type, encrypted_id, 'not_cancelled_monthly_earned_premium' AS errType
from temp_premium_report_us
where encrypted_id in(select encrypted_id from not_cancelled)
group by encrypted_id
having (-0.01) < SUM(monthly_earned_premium) and SUM(monthly_earned_premium) < (0.01)
),
--Flat canclled with written or earned <> 0
flat_cancelled AS(
    SELECT encrypted_id
    from monolith.policies a
    where flat_cancel <> 'FALSE'
    OR id in (select policy_id 
              from policy_premium_activities b where a.id = b.policy_id                 
              and activity='policy_cancelation'
              and metadata:flat_cancel = 'true'
             )
),

flat_cancelled_monthly_written_premium AS(
select sum(monthly_written_premium), encrypted_id, 'flat_cancelled_monthly_written_premium' AS errType
from temp_premium_report_us
where encrypted_id in(select encrypted_id from flat_cancelled)
group by encrypted_id
--having SUM(monthly_written_premium)  <> 0
having (-0.01) > SUM(monthly_written_premium) OR SUM(monthly_written_premium) > (0.01)
),

flat_cancelled_monthly_earned_premium AS(
select sum(monthly_earned_premium), encrypted_id, 'flat_cancelled_monthly_earned_premium' AS errType
from temp_premium_report_us
where encrypted_id in(select encrypted_id from flat_cancelled)
group by encrypted_id
having (-0.01) > SUM(monthly_earned_premium) OR SUM(monthly_earned_premium) > (0.01)
),

-------monthly_unearned_premium < 0
monthly_unearned_premium AS(
select SUM(monthly_unearned_premium), encrypted_id,
    'monthly_unearned_premium < 0'  AS errType
from temp_premium_report_us
group by encrypted_id
having ROUND(SUM(monthly_unearned_premium),2) < (-0.01)
),
-------monthly_earned_premium < 0
monthly_earned_premium AS(
select SUM(monthly_earned_premium), encrypted_id,
    'monthly_earned_premium < 0' AS errType
from temp_premium_report_us
group by encrypted_id
having ROUND(SUM(monthly_earned_premium),2) < (-0.01)
),

-------monthly_written_premium < 0
monthly_written_premium AS(
select SUM(monthly_written_premium), encrypted_id,
    'monthly_written_premium < 0' AS errType
from temp_premium_report_us
group by encrypted_id
having ROUND(SUM(monthly_written_premium),2) < (-0.01) 
),

--Policy is active and written or earned <= 0 
active_policies AS(
    SELECT encrypted_id
    from monolith.policies
    where status='active'
),

active_policies_monthly_written_premium AS(
select sum(monthly_written_premium), encrypted_id,
    'active_policies_monthly_written_premium' AS errType
from temp_premium_report_us
where encrypted_id in(select encrypted_id from active_policies)
group by encrypted_id
having SUM(monthly_written_premium)  <= 0
),
active_policies_monthly_earned_premium AS(
select sum(monthly_earned_premium), encrypted_id,'active_policies_monthly_earned_premium' AS errType
from temp_premium_report_us
where encrypted_id in(select encrypted_id from active_policies)
group by encrypted_id
having SUM(monthly_earned_premium) <= 0
),

--Policy is not active and written <> earned
active_policies AS(
    SELECT encrypted_id
    from monolith.policies
    where status<>'active'
),
inactive_policies_monthly_difference AS(
    select  (sum(monthly_written_premium) - sum(monthly_earned_premium)) AS monthly_sum, encrypted_id, 'active_policies_monthly_difference' AS errType
    from temp_premium_report_us
    where encrypted_id in(select encrypted_id from active_policies)
    group by encrypted_id
    having round(ABS((sum(monthly_written_premium) - sum(monthly_earned_premium))),2) > 0.01
    )
    
SELECT encrypted_id,
       errType
FROM   not_cancelled_monthly_written_premium
UNION
SELECT encrypted_id,
       errType
FROM   flat_cancelled_monthly_written_premium
UNION
SELECT encrypted_id,
       errType
FROM   flat_cancelled_monthly_earned_premium
UNION
SELECT encrypted_id,
       errType
FROM   monthly_unearned_premium
UNION
SELECT encrypted_id,
       errType
FROM   monthly_earned_premium
UNION
SELECT encrypted_id,
       errType
FROM   monthly_written_premium
UNION
SELECT encrypted_id,
       errType
FROM   active_policies_monthly_written_premium
UNION
SELECT encrypted_id,
       errType
FROM   active_policies_monthly_earned_premium
UNION
SELECT encrypted_id,
       errType
FROM   inactive_policies_monthly_difference
ORDER BY 
        errType    

