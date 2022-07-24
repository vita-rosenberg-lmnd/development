WITH temp_premium_report_us AS(
  select 
    * 
  FROM 
    car_finance.premium_report_us
), 

car_not_cancelled AS(
  SELECT 
    public_id 
  FROM 
    car.policies 
  WHERE 
    cancelled_at IS NULL
), 

car_not_cancelled_monthly_written_premium AS(
  SELECT 
    SUM(monthly_written_premium) AS sum_of_type, 
    public_id, 
    'car_not_cancelled_monthly_written_premium' AS errType 
  FROM 
    temp_premium_report_us 
  WHERE 
    public_id IN(
      SELECT 
        public_id 
      FROM 
        car_not_cancelled
    ) 
  GROUP BY 
    public_id 
  HAVING 
    (-0.01) < SUM(monthly_written_premium) 
  AND SUM(monthly_written_premium) < (0.01)
), 

car_not_cancelled_monthly_earned_premium AS(
  SELECT 
    SUM(monthly_earned_premium) AS sum_of_type, 
    public_id, 
    'car_not_cancelled_monthly_earned_premium' AS errType 
  FROM 
    temp_premium_report_us 
  WHERE 
    public_id IN(
      SELECT 
        public_id 
      FROM 
        car_not_cancelled
    ) 
  GROUP BY 
    public_id 
  HAVING 
    (-0.01) < SUM(monthly_earned_premium) 
  AND SUM(monthly_earned_premium) < (0.01)
), 

--car Flat canclled with written or earned <> 0
car_flat_cancelled AS(
  SELECT 
    entity_id AS policyId 
  FROM 
    billing.finance_events activity 
  WHERE 
    activity = 'policy_cancellation' 
    AND metadata : flat = 'true' 
    AND entity_id LIKE 'LCP%'
), 

car_flat_cancelled_monthly_written_premium AS(
  SELECT 
    SUM(monthly_written_premium), 
    public_id, 
    'car_flat_cancelled_monthly_written_premium' errType 
  FROM 
    temp_premium_report_us 
  WHERE 
    public_id IN(
      SELECT 
        policyId 
      FROM 
        car_flat_cancelled
    ) 
  GROUP BY 
    public_id 
  HAVING 
    ABS(SUM(monthly_written_premium)) > 0.01
), 

car_flat_cancelled_monthly_earned_premium AS(
  SELECT 
    SUM(monthly_earned_premium), 
    public_id, 
    'car_flat_cancelled_monthly_earned_premium' errType 
  FROM 
    temp_premium_report_us 
  WHERE 
    public_id IN(
      SELECT 
        policyId 
      FROM 
        car_flat_cancelled
    ) 
  GROUP BY 
    public_id 
  HAVING ABS(SUM(monthly_earned_premium)) > 0.01
), 

--monthly_unearned_premium < 0
monthly_unearned_premium AS(
  SELECT 
    SUM(monthly_unearned_premium), 
    public_id, 
    'monthly_unearned_premium < 0' AS errType 
  FROM 
    temp_premium_report_us 
  GROUP BY 
    public_id 
  HAVING ROUND(SUM(monthly_unearned_premium), 2) < (-0.01)
), 

--monthly_earned_premium < 0
monthly_earned_premium AS(
  SELECT 
    SUM(monthly_earned_premium), 
    public_id, 
    'monthly_earned_premium < 0' AS errType 
  FROM 
    temp_premium_report_us 
  GROUP BY 
    public_id 
  HAVING 
    ROUND(SUM(monthly_earned_premium), 2) < (-0.01)
), 

--monthly_written_premium < 0
monthly_written_premium AS(
  SELECT 
    SUM(monthly_written_premium), 
    public_id, 
    'monthly_written_premium < 0' AS errType 
  FROM 
    temp_premium_report_us 
  GROUP BY 
    public_id 
  HAVING 
    ROUND(SUM(monthly_written_premium), 2) < (-0.01)
), 

--car Policy is active and written or earned <= 0
car_active_policies AS(
  SELECT 
    public_id 
  FROM 
    car.policies 
  WHERE 
    status = 'active'
), 

car_active_policies_monthly_written_premium AS(
  SELECT 
    SUM(monthly_written_premium), 
    public_id, 
    'car_active_policies_monthly_written_premium' AS errType 
  FROM 
    temp_premium_report_us 
  WHERE 
    public_id IN(
      SELECT 
        public_id 
      FROM 
        car_active_policies
    ) 
  GROUP BY 
    public_id 
  HAVING 
    SUM(monthly_written_premium) <= 0
), 

car_active_policies_monthly_earned_premium AS(
  SELECT 
    SUM(monthly_earned_premium), 
    public_id, 
    'car_active_policies_monthly_earned_premium' AS errType 
  FROM 
    temp_premium_report_us 
  WHERE 
    public_id IN(
      SELECT 
        public_id 
      FROM 
        car_active_policies
    ) 
  GROUP BY 
    public_id 
  HAVING 
    SUM(monthly_earned_premium) <= 0
), 

--car Policy is not active and written <> earned
car_inactive_policies AS(
  SELECT 
    public_id 
  FROM 
    car.policies 
  WHERE 
    status <> 'active'
), 

car_inactive_policies_monthly_difference AS(
  SELECT 
    (
      SUM(monthly_written_premium) - SUM(monthly_earned_premium)
    ) AS monthly_sum, 
    public_id, 
    'car_inactive_policies_monthly_difference' AS errType 
  FROM 
    temp_premium_report_us 
  WHERE 
    public_id IN(
      SELECT 
        public_id 
      FROM 
        car_inactive_policies
    ) 
  GROUP BY 
    public_id 
  HAVING 
    ROUND(ABS((SUM(monthly_written_premium) - SUM(monthly_earned_premium))), 2) > 0.01
) 
SELECT 
  public_id, 
  errType 
FROM 
  car_not_cancelled_monthly_written_premium 
UNION 
SELECT 
  public_id, 
  errType 
FROM 
  car_flat_cancelled_monthly_written_premium 
UNION 
SELECT 
  public_id, 
  errType 
FROM 
  car_flat_cancelled_monthly_earned_premium 
UNION 
SELECT 
  public_id, 
  errType 
FROM 
  monthly_unearned_premium 
UNION 
SELECT 
  public_id, 
  errType 
FROM 
  monthly_earned_premium 
UNION 
SELECT 
  public_id, 
  errType 
FROM 
  monthly_written_premium 
UNION 
SELECT 
  public_id, 
  errType 
FROM 
  car_active_policies_monthly_written_premium 
UNION 
SELECT 
  public_id, 
  errType 
FROM 
  car_active_policies_monthly_earned_premium 
UNION 
SELECT 
  public_id, 
  errType 
FROM 
  car_inactive_policies_monthly_difference 
ORDER BY 
  errType
