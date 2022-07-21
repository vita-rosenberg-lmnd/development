--set tableName = 'CAR_FINANCE.tmpPREMIUM_REPORT_US';
--create or replace temporary table identifier($tableName)
--AS
--select * from CAR_FINANCE.PREMIUM_REPORT_US;

--car Not canclled with written or earned = 0
with car_not_cancelled AS(
    SELECT public_id
    from car.policies
    where cancelled_at is null
),
car_not_cancelled_monthly_written_premium AS(
select sum(monthly_written_premium) AS sum_of_type, public_id, 
    'car_not_cancelled_monthly_written_premium' AS errType
from identifier($tableName)
where public_id in(select public_id from car_not_cancelled)
group by public_id
having (-0.01) < SUM(monthly_written_premium) and SUM(monthly_written_premium) < (0.01))
,
car_not_cancelled_monthly_earned_premium AS(
select sum(monthly_earned_premium) AS sum_of_type, public_id, 
    'car_not_cancelled_monthly_earned_premium' AS errType
from identifier($tableName)
where public_id in(select public_id from car_not_cancelled)
group by public_id
having (-0.01) < SUM(monthly_earned_premium) and SUM(monthly_earned_premium) < (0.01)
),
--car Flat canclled with written or earned <> 0
car_flat_cancelled AS(
    SELECT entity_id AS policyId
    FROM billing.finance_events activity
    where activity= 'policy_cancellation' 
    and metadata:flat='true'
    and entity_id like 'LCP%'
),
car_flat_cancelled_monthly_written_premium AS(
SELECT sum(monthly_written_premium), public_id, 'car_flat_cancelled_monthly_written_premium' errType
from identifier($tableName)
where public_id in(select policyId from car_flat_cancelled)
group by public_id
having ABS(SUM(monthly_written_premium))  > 0.01
    ),
car_flat_cancelled_monthly_earned_premium AS(
select sum(monthly_earned_premium), public_id,'car_flat_cancelled_monthly_earned_premium' errType
from identifier($tableName)
where public_id in(select policyId from car_flat_cancelled)
group by public_id
having ABS(SUM(monthly_earned_premium)) > 0.01 
),
--monthly_unearned_premium < 0
monthly_unearned_premium AS(
select SUM(monthly_unearned_premium), public_id, 'monthly_unearned_premium < 0' AS errType
from identifier($tableName)
group by public_id
having ROUND(SUM(monthly_unearned_premium),2) < (-0.01)
),
--monthly_earned_premium < 0
monthly_earned_premium AS(
select SUM(monthly_earned_premium), public_id, 'monthly_earned_premium < 0' AS errType
from identifier($tableName)
group by public_id
having ROUND(SUM(monthly_earned_premium),2) < (-0.01)
),
--monthly_written_premium < 0
monthly_written_premium AS(
select SUM(monthly_written_premium), public_id, 'monthly_written_premium < 0' AS errType
from identifier($tableName)
group by public_id
having ROUND(SUM(monthly_written_premium),2) < (-0.01)
),
--car Policy is active and written or earned <= 0  
car_active_policies AS(
select public_id 
from car.policies
where status = 'active'
),
car_active_policies_monthly_written_premium AS(
select sum(monthly_written_premium), public_id,'car_active_policies_monthly_written_premium' AS errType
from identifier($tableName)
where public_id in(select public_id from car_active_policies)
group by public_id
having SUM(monthly_written_premium)  <= 0
),
car_active_policies_monthly_earned_premium AS(
select sum(monthly_earned_premium), public_id,'car_active_policies_monthly_earned_premium' AS errType
from identifier($tableName)
where public_id in(select public_id from car_active_policies)
group by public_id
having SUM(monthly_earned_premium) <= 0
),
--car Policy is not active and written <> earned
car_inactive_policies AS(
    SELECT public_id
    from car.policies
    where status<>'active'
),
car_inactive_policies_monthly_difference as(
    select  (sum(monthly_written_premium) - sum(monthly_earned_premium)) AS monthly_sum, public_id,
    'car_inactive_policies_monthly_difference' AS errType
    from identifier($tableName)
    where public_id in(select public_id from car_inactive_policies)
    group by public_id
    having round(ABS((sum(monthly_written_premium) - sum(monthly_earned_premium))),2) > 0.01
    )
    
      SELECT public_id, errType FROM car_not_cancelled_monthly_written_premium
UNION SELECT public_id, errType FROM car_flat_cancelled_monthly_written_premium
UNION SELECT public_id, errType FROM car_flat_cancelled_monthly_earned_premium
UNION SELECT public_id, errType FROM monthly_unearned_premium
UNION SELECT public_id, errType FROM monthly_earned_premium
UNION SELECT public_id, errType FROM monthly_written_premium
UNION SELECT public_id, errType FROM car_active_policies_monthly_written_premium
UNION SELECT public_id, errType FROM car_active_policies_monthly_earned_premium
UNION SELECT public_id, errType FROM car_inactive_policies_monthly_difference
order by errType
