--select * from public.earthquake_report where policy_id = 5227949 order by month, policy_version_id limit 100;


with written_cte AS
    (
    SELECT--written
    p.policy_id,
    TO_CHAR(year(p.accounting_date)) AS year,
    TO_CHAR(month(p.accounting_date)) AS month,
    SUM(p.value) AS premium_activities
        FROM public.policy_premium_activities AS p
        WHERE COALESCE(created_at, DATE_TRUNC('month', CURRENT_DATE)::date) < DATEADD(hour, 12, DATE_TRUNC('month', CURRENT_DATE)::date)
        GROUP BY policy_id, month, year 
    ),
    
--select * from written_cte

earned_cte AS
    (
    SELECT--earned--every day for yesterday
    f.policy_id,
    TO_CHAR(year(f.accounting_date)) AS year,
    TO_CHAR(month(f.accounting_date)) AS month, 
    SUM(f.value) AS earned_premium
        FROM finance.daily_earned_premium AS f
        WHERE COALESCE(created_at, DATE_TRUNC('month', CURRENT_DATE)::date) < DATEADD(hour, 12, DATE_TRUNC('month', CURRENT_DATE)::date)
        GROUP BY policy_id, month, year
    ),
    
combined AS(
    SELECT
    COALESCE(a.policy_id, b.policy_id) AS policy_id,
    COALESCE(a.month, b.month) AS month,
    COALESCE(a.year, b.year) AS year,
    --LAST_DAY(CAST(DATE_TRUNC('quarter', COALESCE(a.month, b.month)) AS date) + INTERVAL '2 Months') AS quarter,
    COALESCE(a.premium_activities, 0) AS policy_monthly_premium_activites,
    COALESCE(b.earned_premium, 0) AS policy_monthly_earned_premium
    --SUM(policy_monthly_premium_activites) OVER(PARTITION BY COALESCE(a.policy_id, b.policy_id), quarter) AS policy_quarterly_premium_activites,
    --SUM(policy_monthly_earned_premium) OVER(PARTITION BY COALESCE(a.policy_id, b.policy_id), quarter) AS policy_quarterly_earned_premium
    FROM written_cte a
        FULL OUTER JOIN earned_cte b
            ON (a.policy_id = b.policy_id
            AND a.month = b.month
            AND a.year=b.year)
)/*
premium_full as(
SELECT 
c.policy_id,
c.month as date,
c.quarter,
policy_monthly_premium_activites,
policy_monthly_earned_premium,
p.country
    FROM combined c
        JOIN monolith.policies AS p
        ON p.id = c.policy_id
            WHERE p.status != 'pending'
            AND p.test = 0
    )*/
    
select * from combined
where policy_id=5227949
