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
    'Not canclled with written or earned = 0' AS errtype 
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
    SUM(monthly_written_premium) = 0
), 

not_cancelled_monthly_earned_premium AS (
  SELECT 
    SUM(monthly_earned_premium) AS sum_of_type, 
    encrypted_id, 
    'Not canclled with written or earned = 0' AS errtype 
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
    SUM(monthly_earned_premium)=0
), 

--Flat canclled with written or earned <> 0
flat_cancelled AS (
  SELECT 
    encrypted_id 
  FROM 
    monolith.policies a 
  WHERE 
    flat_cancel <> 'FALSE' 
), 

flat_cancelled_monthly_written_premium AS (
  SELECT 
    SUM(monthly_written_premium), 
    encrypted_id, 
    'Flat canclled with written or earned <> 0' AS errtype 
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
    SUM(monthly_written_premium) <> 0
), 

flat_cancelled_monthly_earned_premium AS (
  SELECT 
    SUM(monthly_earned_premium), 
    encrypted_id, 
    'Flat canclled with written or earned <> 0' AS errtype 
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
    SUM(monthly_earned_premium) <> 0
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
    SUM(monthly_unearned_premium) < 0
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
    SUM(monthly_earned_premium) < 0
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
    SUM(monthly_written_premium) < 0
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
    'Policy is active and written or earned <= 0' AS errtype 
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
    'Policy is active and written or earned <= 0' AS errtype 
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
    'Policy is not active and written <> earned' AS errtype 
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
  HAVING (SUM(monthly_written_premium) - SUM(monthly_earned_premium)) <> 0
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
