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
    WHERE endorsement_type = 'lemonade_earthquake'
),

calc_index_cte AS(
select
    policy_version_id,
    endorsement_value,--    calc_index,
    ROW_NUMBER() OVER (PARTITION BY policy_version_id ORDER BY calc_index DESC) AS row_number_of_calc_index
from policy_endorsements_cte 
)

--select * from calc_index_cte
,

base_sum_cte AS(
select policy_version_id,
       endorsement_value AS annual_sum,--calc_index,
       row_number_of_calc_index
from calc_index_cte
where row_number_of_calc_index = 1
),

dates_cte AS(
SELECT pv.policy_id, 
    policy_version_id, 
    annual_sum,
    pv.start_date,
    convert_timezone('UTC', t.timezone, pv.start_date) AS start_date_local,
    --coalesce(pv.end_date, '2099-01-01') AS end_date,
    coalesce(pv.end_date, CURRENT_DATE()) AS end_date,    
    pv.end_date AS end_date_original,
    convert_timezone('UTC', t.timezone, end_date) AS end_date_local,    
    pv.state    
FROM base_sum_cte bc join monolith.policy_versions pv on bc.policy_version_id = pv.id
    join timezones t on pv.state=t.state_code
),

date_diff_cte AS (
select 
    policy_id, 
    policy_version_id, 
    annual_sum, 
    start_date_local,
    start_date, 
    end_date_local,
    end_date, 
    datediff(day,start_date_local, end_date_local) num_of_days_local,
    datediff(day,start_date, end_date) num_of_days, 
    state
from dates_cte
),
 
flat_versions AS (select policy_id, 
       policy_version_id,
       annual_sum,
       DATE(start_date) AS start_date,
       --start_date_local AS start_date_time_local,
       DATE(start_date_local) AS start_date_local,
       DATE(end_date) AS end_date,
       --end_date_local AS end_date_time_local,
       DATE(end_date_local) AS end_date_local,
       num_of_days,
       num_of_days_local, 
       state
from date_diff_cte dd-- join timezones t on dd.state=t.state_code
--where policy_id = 5227949
--WHERE num_of_days_UTC<>num_of_days
),

a_few_versions_a_day AS(
select policy_id, 
       policy_version_id,
       ROW_NUMBER() OVER (PARTITION BY policy_id, start_date_local ORDER BY policy_version_id DESC) AS Last_version_a_day,
       ROW_NUMBER() OVER (PARTITION BY policy_id ORDER by policy_version_id DESC) AS last_version_per_policy,
       --daily_sum, 
       --year,
       --num_of_days_in_year,
       annual_sum,
       start_date,
       start_date_local,
       end_date,
       end_date_local,
       num_of_days,
       num_of_days_local,
       state
from flat_versions  
),
    
one_version_per_day AS(
select policy_id, 
       policy_version_id,
       --Last_version_a_day,
       last_version_per_policy,
       --daily_sum, 
       --year,
       --num_of_days_in_year,
       annual_sum,
       start_date,
       start_date_local,
       --start_date_time_local,
       end_date,
       end_date_local,
       --end_date_time_local,
       num_of_days,
       num_of_days_local,
       state   
from  a_few_versions_a_day
where Last_version_a_day = 1
),

num_of_days_last_version_plus_one AS (
select policy_id, 
       policy_version_id,
       IFF(last_version_per_policy = 1, num_of_days + 1, num_of_days) AS num_of_days_plus_one_day,
       --year,
       annual_sum,
       start_date,
       start_date_local,
       --start_date_time_local,
       --LAST_DAY(DATE_TRUNC('month', start_date_time_UTC)::date) AS start_month_local,
       end_date,
       end_date_local,
       --end_date_time_local,
       --LAST_DAY(DATE_TRUNC('month', end_date_time_UTC)::date) AS end_month_local,
       num_of_days,
       num_of_days_local,
       a.state,
       years_insured  
FROM   one_version_per_day a join monolith.policies b on a.policy_id=b.id   
),

flatten_sum_for_days AS(
select policy_id, 
       policy_version_id,
       annual_sum,
       start_date,
       start_date_local,
       --start_month_local,
       end_date,
       end_date_local,
       --end_month_local,
       num_of_days,
       num_of_days_local,
       state,
       years_insured,--not this
       t.date_date,
       extract(year from t.date_date) AS year_,
       IFF(year_%4 = 0 , 366, 365) AS num_of_days_in_year,--change effective date plus year 
       annual_sum / num_of_days_in_year as daily_sum
from num_of_days_last_version_plus_one l join dim_date_cte t 
     ON l.start_date_local<=t.date_date AND t.date_date<end_date_local
 WHERE policy_id=2523935-- and policy_version_id<>25695886
)

select * from flatten_sum_for_days
;

SELECT SUM(daily_sum) AS monthly_written,
       start_date_local,
       end_date_local,
       to_varchar(date_date::date, 'yyyy - mm') AS year_month,
       policy_id, 
       policy_version_id
FROM flatten_sum_for_days where policy_id = 5227949
       group by year_month, policy_id, policy_version_id,start_date_local,end_date_local
       order by policy_version_id, year_month
       
     --where amount_per_active_days <> amount_per_active_days_plus_one


;
select * from 
public.policy_premium_activities-- limit 100
where policy_id = 5227949
order by activity_date
;
SELECT CAST(2,022 AS VARCHAR) 
select date_part(year, CURRENT_DATE())
select to_varchar(2,022::date, 'mon  yyyy')
select * from public.earthquake_report where policy_id = 5227949 order by month, policy_version_id limit 100;
select * from public.earthquake_report where policy_id = 4306259 order by month, policy_version_id limit 100;
select * from monolith.policy_versions where id = 23055257
where end_date is null

select datediff(day,'2022-02-22 00:01:00.000','2022-03-08 04:10:51.000')--14
select datediff(day,'2022-03-08 04:10:52.000','2022-03-12 14:05:20.000')--4
select datediff(day,'2022-03-12 14:05:21.000','2022-04-02 04:43:06.902')--21 -> 22

;
select * from monolith.policy_versions
where YEAR(start_date) <> year(end_date)
;
monolith.policy_version_premiums a 
join monolith.policy_versions p 
on a.policy_version_id=p.id
where end_date is null
limit 100

select * from 
monolith.policy_version_premiums a 
where policy_version_id = 22068486


 
SELECT CASE WHEN ISDATE(CAST('2022' AS char(4)) + '0229') = 1 THEN 'LEAP YEAR' ELSE 'NORMAL YEAR' END
select 2024%4

;
select * from LEMONADE.FINANCE.EVENTS where policy_id=5227949
limit 100
;
select * from monolith.policies 
WHERE id=5227949
;
select * from billing.finance_events a join monolith.policies b on a.entity_id = b.encrypted_id
where b.id=5227949
;
select * from billing.finance_events
where policy_id=5227949
;
select extract(year from to_timestamp('2022-05-08T23:39:20.123-07:00'))/365 as v
select to_timestamp('2022-05-08T23:39:20.123-07:00')
select extract(year from current_date())
SELECT CASE WHEN ISDATE(CAST('2022' AS char(4)) + '0229') = 1 THEN 'LEAP YEAR' ELSE 'NORMAL YEAR' END

select extract(year from 2022)

select to_varchar('2013-04-05'::date, 'yyyy - mm')






select * from earthquake_report where month = '2022-06-01' and policy_version_id = 21179095



select * from public.master_policies
limit 100

