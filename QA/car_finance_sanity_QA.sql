set tableName = 'CAR_FINANCE.PREMIUM_REPORT_US';
create or replace temporary table identifier($tableName)
AS
select * from CAR_FINANCE.PREMIUM_REPORT_US

--Not canclled with written or earned = 0
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
