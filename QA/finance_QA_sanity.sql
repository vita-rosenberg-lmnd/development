set tableName = 'LEMONADE_DEVELOPMENT.FINANCE.PREMIUM_REPORT_US';
create or replace table identifier($tableName)
AS
select * from LEMONADE.FINANCE.PREMIUM_REPORT_US

--Not canclled with written or earned = 0
with not_cancelled AS(
    SELECT encrypted_id
    from monolith.policies
    where canceled_date IS NULL
    and effective_date<current_date()
)
select sum(monthly_written_premium) AS sum_of_type, encrypted_id, 'monthly_written_premium' type
from identifier($tableName)
where encrypted_id in(select encrypted_id from not_cancelled)
group by encrypted_id
having (-0.01) < SUM(monthly_written_premium) and SUM(monthly_written_premium) < (0.01)
UNION
select sum(monthly_earned_premium) AS sum_of_type, encrypted_id, 'monthly_earned_premium' type
from identifier($tableName)
where encrypted_id in(select encrypted_id from not_cancelled)
group by encrypted_id
having (-0.01) < SUM(monthly_earned_premium) and SUM(monthly_earned_premium) < (0.01)

--Flat canclled with written or earned <> 0
with flat_cancelled AS(
    SELECT encrypted_id
    from monolith.policies a
    where flat_cancel <> 'FALSE'
    OR id in (select policy_id 
              from policy_premium_activities b where a.id = b.policy_id                 
              and activity='policy_cancelation'
              and metadata:flat_cancel = 'true'
             )
)
select sum(monthly_written_premium), encrypted_id,'monthly_written_premium' type
from identifier($tableName)
where encrypted_id in(select encrypted_id from flat_cancelled)
group by encrypted_id
--having SUM(monthly_written_premium)  <> 0
having (-0.01) > SUM(monthly_written_premium) OR SUM(monthly_written_premium) > (0.01)
UNION
select sum(monthly_earned_premium), encrypted_id,'monthly_earned_premium' type
from identifier($tableName)
where encrypted_id in(select encrypted_id from flat_cancelled)
group by encrypted_id
having (-0.01) > SUM(monthly_earned_premium) OR SUM(monthly_earned_premium) > (0.01)

-------monthly_unearned_premium < 0
select SUM(monthly_unearned_premium), encrypted_id
from LEMONADE_DEVELOPMENT.FINANCE.PREMIUM_REPORT_US
group by encrypted_id
having ROUND(SUM(monthly_unearned_premium),2) < (-0.01)

-------monthly_earned_premium < 0
select SUM(monthly_earned_premium), encrypted_id
from LEMONADE_DEVELOPMENT.FINANCE.PREMIUM_REPORT_US
group by encrypted_id
having ROUND(SUM(monthly_earned_premium),2) < (-0.01)
 
-------monthly_written_premium < 0
select SUM(monthly_written_premium), encrypted_id
from LEMONADE_DEVELOPMENT.FINANCE.PREMIUM_REPORT_US
group by encrypted_id
having ROUND(SUM(monthly_written_premium),2) < (-0.01) 

--Policy is active and written or earned <= 0 
with active_policies AS(
    SELECT encrypted_id
    from monolith.policies
    where status='active'
)
select sum(monthly_written_premium), encrypted_id,'monthly_written_premium' type
from LEMONADE_DEVELOPMENT.FINANCE.PREMIUM_REPORT_US--44,867,215
where encrypted_id in(select encrypted_id from active_policies)
group by encrypted_id
having SUM(monthly_written_premium)  <= 0
UNION
select sum(monthly_earned_premium), encrypted_id,'monthly_earned_premium' type
from LEMONADE_DEVELOPMENT.FINANCE.PREMIUM_REPORT_US--44,867,215
where encrypted_id in(select encrypted_id from active_policies)
group by encrypted_id
having SUM(monthly_earned_premium) <= 0

--Policy is not active and written <> earned
with active_policies AS(
    SELECT encrypted_id
    from monolith.policies
    where status<>'active'
),
    monthly_difference as(
    select  (sum(monthly_written_premium) - sum(monthly_earned_premium)) AS monthly_sum, encrypted_id 
    from LEMONADE_DEVELOPMENT.FINANCE.PREMIUM_REPORT_US
    where encrypted_id in(select encrypted_id from active_policies)
    group by encrypted_id
    having round(ABS((sum(monthly_written_premium) - sum(monthly_earned_premium))),2) > 0.01
    )
select encrypted_id,monthly_sum
from monthly_difference

-------

