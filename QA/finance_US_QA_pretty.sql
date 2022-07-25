WITH temp_premium_report_us AS (
  SELECT 
    * 
  FROM 
    finance.premium_report_us
), 

--Not canclled with written or earned = 0
not_cancelled AS (
  SELECT 
    encrypted_id 
  FROM 
    monolith.policies 
  WHERE 
    canceled_date IS NULL 
  AND effective_date < CURRENT_DATE()
), 

not_cancelled_monthly_written_premium AS (
  SELECT 
    SUM(monthly_written_premium) AS sum_of_type, 
    encrypted_id, 
    'not_cancelled_monthly_written_premium' AS errtype 
  FROM 
    temp_premium_report_us 
  WHERE 
    encrypted_id IN (
      SELECT 
        encrypted_id 
      FROM 
        not_cancelled
    ) 
  GROUP BY 
    encrypted_id 
  HAVING -- there is no approve for the 0.01
    (-0.01) < SUM(monthly_written_premium) 
  AND SUM(monthly_written_premium) < (0.01)
), 

not_cancelled_monthly_earned_premium AS (
  SELECT 
    SUM(monthly_earned_premium) AS sum_of_type, 
    encrypted_id, 
    'not_cancelled_monthly_earned_premium' AS errtype 
  FROM 
    temp_premium_report_us 
  WHERE 
    encrypted_id IN (
      SELECT 
        encrypted_id 
      FROM 
        not_cancelled
    ) 
  GROUP BY 
    encrypted_id 
  HAVING 
    (-0.01) < SUM(monthly_earned_premium) 
  AND SUM(monthly_earned_premium) < (0.01)
), 

--Flat canclled with written or earned <> 0
flat_cancelled AS (
  SELECT 
    encrypted_id 
  FROM 
    monolith.policies a 
  WHERE 
    flat_cancel <> 'FALSE' 
    OR id IN (
      SELECT 
        policy_id 
      FROM 
        policy_premium_activities b 
      WHERE 
        a.id = b.policy_id 
        AND activity = 'policy_cancelation' 
        AND METADATA : flat_cancel = 'true'
    )
), 

flat_cancelled_monthly_written_premium AS (
  SELECT 
    SUM(monthly_written_premium), 
    encrypted_id, 
    'flat_cancelled_monthly_written_premium' AS errtype 
  FROM 
    temp_premium_report_us 
  WHERE 
    encrypted_id IN (
      SELECT 
        encrypted_id 
      FROM 
        flat_cancelled
    ) 
  GROUP BY 
    encrypted_id --having SUM(monthly_written_premium)  <> 0
  HAVING 
    (-0.01) > SUM(monthly_written_premium) 
  OR SUM(monthly_written_premium) > (0.01)
), 

flat_cancelled_monthly_earned_premium AS (
  SELECT 
    SUM(monthly_earned_premium), 
    encrypted_id, 
    'flat_cancelled_monthly_earned_premium' AS errtype 
  FROM 
    temp_premium_report_us 
  WHERE 
    encrypted_id IN (
      SELECT 
        encrypted_id 
      FROM 
        flat_cancelled
    ) 
  GROUP BY 
    encrypted_id 
  HAVING 
    (-0.01) > SUM(monthly_earned_premium) 
  OR SUM(monthly_earned_premium) > (0.01)
), 

-------monthly_unearned_premium < 0
monthly_unearned_premium AS (
  SELECT 
    SUM(monthly_unearned_premium), 
    encrypted_id, 
    'monthly_unearned_premium < 0' AS errtype 
  FROM 
    temp_premium_report_us 
  GROUP BY 
    encrypted_id 
  HAVING 
    ROUND(SUM(monthly_unearned_premium), 2) < (-0.01)
), 

-------monthly_earned_premium < 0
monthly_earned_premium AS (
  SELECT 
    SUM(monthly_earned_premium), 
    encrypted_id, 
    'monthly_earned_premium < 0' AS errtype 
  FROM 
    temp_premium_report_us 
  GROUP BY 
    encrypted_id 
  HAVING 
    ROUND(SUM(monthly_earned_premium), 2) < (-0.01)
), 

-------monthly_written_premium < 0
monthly_written_premium AS (
  SELECT 
    SUM(monthly_written_premium), 
    encrypted_id, 
    'monthly_written_premium < 0' AS errtype 
  FROM 
    temp_premium_report_us 
  GROUP BY 
    encrypted_id 
  HAVING 
    ROUND(SUM(monthly_written_premium), 2) < (-0.01)
), 

--Policy is active and written or earned <= 0
active_policies AS (
  SELECT 
    encrypted_id 
  FROM 
    monolith.policies 
  WHERE 
    status = 'active'
), 

active_policies_monthly_written_premium AS (
  SELECT 
    SUM(monthly_written_premium), 
    encrypted_id, 
    'active_policies_monthly_written_premium' AS errtype 
  FROM 
    temp_premium_report_us 
  WHERE 
    encrypted_id IN (
      SELECT 
        encrypted_id 
      FROM 
        active_policies
    ) 
  GROUP BY 
    encrypted_id 
  HAVING 
    SUM(monthly_written_premium) <= 0
), 

active_policies_monthly_earned_premium AS (
  SELECT 
    SUM(monthly_earned_premium), 
    encrypted_id, 
    'active_policies_monthly_earned_premium' AS errtype 
  FROM 
    temp_premium_report_us 
  WHERE 
    encrypted_id IN (
      SELECT 
        encrypted_id 
      FROM 
        active_policies
    ) 
  GROUP BY 
    encrypted_id 
  HAVING 
    SUM(monthly_earned_premium) <= 0
), 

--Policy is not active and written <> earned
inactive_policies AS (
  SELECT 
    encrypted_id 
  FROM 
    monolith.policies 
  WHERE 
    status <> 'active'
), 
inactive_policies_monthly_difference AS (
  SELECT 
    (
      SUM(monthly_written_premium) - SUM(monthly_earned_premium)
    ) AS monthly_sum, 
    encrypted_id, 
    'active_policies_monthly_difference' AS errtype 
  FROM 
    temp_premium_report_us 
  WHERE 
    encrypted_id IN (
      SELECT 
        encrypted_id 
      FROM 
        inactive_policies
    ) 
  GROUP BY 
    encrypted_id 
  HAVING round(abs((SUM(monthly_written_premium) - SUM(monthly_earned_premium))), 2) > 0.01
) 

SELECT 
  encrypted_id, 
  errtype 
FROM 
  not_cancelled_monthly_written_premium 
UNION 
SELECT 
  encrypted_id, 
  errtype 
FROM 
  flat_cancelled_monthly_written_premium 
UNION 
SELECT 
  encrypted_id, 
  errtype 
FROM 
  flat_cancelled_monthly_earned_premium 
UNION 
SELECT 
  encrypted_id, 
  errtype 
FROM 
  monthly_unearned_premium 
UNION 
SELECT 
  encrypted_id, 
  errtype 
FROM 
  monthly_earned_premium 
UNION 
SELECT 
  encrypted_id, 
  errtype 
FROM 
  monthly_written_premium 
UNION 
SELECT 
  encrypted_id, 
  errtype 
FROM 
  active_policies_monthly_written_premium 
UNION 
SELECT 
  encrypted_id, 
  errtype 
FROM 
  active_policies_monthly_earned_premium 
UNION 
SELECT 
  encrypted_id, 
  errtype 
FROM 
  inactive_policies_monthly_difference 
ORDER BY 
  errtype
