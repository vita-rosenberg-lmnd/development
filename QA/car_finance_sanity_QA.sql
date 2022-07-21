--
--car_finance.premium_report_us
--

set tableName = 'CAR_FINANCE.PREMIUM_REPORT_US';
create or replace temporary table identifier($tableName)
AS
select * from CAR_FINANCE.PREMIUM_REPORT_US

--car Not canclled with written or earned = 0
with not_cancelled AS(
    SELECT public_id
    from car.policies
    where cancelled_at is null
)
select sum(monthly_written_premium) AS sum_of_type, public_id, 'monthly_written_premium' type
from identifier($tableName)
where public_id in(select public_id from not_cancelled)
group by public_id
having (-0.01) < SUM(monthly_written_premium) and SUM(monthly_written_premium) < (0.01)
UNION
select sum(monthly_earned_premium) AS sum_of_type, public_id, 'monthly_earned_premium' type
from identifier($tableName)
where public_id in(select public_id from not_cancelled)
group by public_id
having (-0.01) < SUM(monthly_earned_premium) and SUM(monthly_earned_premium) < (0.01)

--pet Flat canclled with written or earned <> 0
WITH pet_flat_cancelled AS(
    SELECT entity_id AS policyId
    FROM billing.finance_events activity
    where activity= 'policy_cancellation' 
    and metadata:flat='true'
)
SELECT sum(monthly_written_premium), public_id, 'monthly_written_premium' type
from identifier($tableName)
where public_id in(select policyId from pet_flat_cancelled)
group by public_id
having ABS(SUM(monthly_written_premium))  > 0.01
UNION
select sum(monthly_earned_premium), public_id,'monthly_earned_premium' type
from identifier($tableName)
where public_id in(select policyId from pet_flat_cancelled)
group by public_id
having ABS(SUM(monthly_earned_premium)) > 0.01 

--monthly_unearned_premium < 0
select SUM(monthly_unearned_premium), public_id
from identifier($tableName)
group by public_id
having ROUND(SUM(monthly_unearned_premium),2) < (-0.01)

--monthly_earned_premium < 0
select SUM(monthly_earned_premium), public_id
from identifier($tableName)
group by public_id
having ROUND(SUM(monthly_earned_premium),2) < (-0.01)

--monthly_written_premium < 0
select SUM(monthly_written_premium), public_id
from identifier($tableName)
group by public_id
having ROUND(SUM(monthly_written_premium),2) < (-0.01)

--car Policy is active and written or earned <= 0 
with car_active_policies AS(
select public_id 
from car.policies
where status = 'active'
)
select sum(monthly_written_premium), public_id,'monthly_written_premium' type
from identifier($tableName)
where public_id in(select public_id from car_active_policies)
group by public_id
having SUM(monthly_written_premium)  <= 0
UNION
select sum(monthly_earned_premium), public_id,'monthly_earned_premium' type
from identifier($tableName)
where public_id in(select public_id from car_active_policies)
group by public_id
having SUM(monthly_earned_premium) <= 0

--car Policy is not active and written <> earned
with car_active_policies AS(
    SELECT public_id
    from car.policies
    where status<>'active'
),
    monthly_difference as(
    select  (sum(monthly_written_premium) - sum(monthly_earned_premium)) AS monthly_sum, public_id 
    from identifier($tableName)
    where public_id in(select public_id from car_active_policies)
    group by public_id
    having round(ABS((sum(monthly_written_premium) - sum(monthly_earned_premium))),2) > 0.01
    )
    
select public_id, monthly_sum
from monthly_difference
