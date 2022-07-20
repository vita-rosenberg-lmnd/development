--pet_finance.premium_report_us
create or replace table identifier($tableName)
AS
select * from LEMONADE.PET_FINANCE.PREMIUM_REPORT_US;

--Not canclled with written or earned = 0
with not_cancelled AS(
select public_id 
from pet.policies
where canceled_at is null
)

select sum(monthly_written_premium), public_id,'monthly_written_premium' type
from identifier($tableName)
where public_id in(select public_id from not_cancelled)
group by public_id
having ROUND(SUM(monthly_written_premium),2) < (-0.01)
UNION
select sum(monthly_earned_premium), public_id,'monthly_earned_premium' type
from identifier($tableName)
where public_id in(select public_id from not_cancelled)
group by public_id
having ROUND(SUM(monthly_earned_premium),2) < (-0.01);

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

