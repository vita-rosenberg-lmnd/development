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
    'Not canclled with written or earned = 0' AS errType 
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
    SUM(monthly_written_premium) = 0
), 

car_not_cancelled_monthly_earned_premium AS(
  SELECT 
    SUM(monthly_earned_premium) AS sum_of_type, 
    public_id, 
    'Not canclled with written or earned = 0' AS errType 
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
    SUM(monthly_earned_premium) = 0
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
    SUM(monthly_written_premium) AS sum_of_type, 
    public_id, 
    'Flat canclled with written or earned <> 0' errType 
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
    SUM(monthly_written_premium) <> 0
), 

car_flat_cancelled_monthly_earned_premium AS(
  SELECT 
    SUM(monthly_earned_premium) AS sum_of_type, 
    public_id, 
    'Flat canclled with written or earned <> 0' errType 
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
    SUM(monthly_earned_premium) <> 0
), 

--monthly_unearned_premium < 0
monthly_unearned_premium AS(
  SELECT 
    SUM(monthly_unearned_premium) AS sum_of_type, 
    public_id, 
    'monthly_unearned_premium < 0' AS errType 
  FROM 
    temp_premium_report_us 
  GROUP BY 
    public_id 
  HAVING 
    SUM(monthly_unearned_premium) < 0
), 

--monthly_earned_premium < 0
monthly_earned_premium AS(
  SELECT 
    SUM(monthly_earned_premium) AS sum_of_type, 
    public_id, 
    'monthly_earned_premium < 0' AS errType 
  FROM 
    temp_premium_report_us 
  GROUP BY 
    public_id 
  HAVING 
    SUM(monthly_earned_premium) < 0
), 

--monthly_written_premium < 0
monthly_written_premium AS(
  SELECT 
    SUM(monthly_written_premium) AS sum_of_type, 
    public_id, 
    'monthly_written_premium < 0' AS errType 
  FROM 
    temp_premium_report_us 
  GROUP BY 
    public_id 
  HAVING 
    SUM(monthly_written_premium) < 0
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
    SUM(monthly_written_premium) AS sum_of_type, 
    public_id, 
    'Policy is active and written or earned <= 0 ' AS errType 
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
    SUM(monthly_earned_premium) AS sum_of_type, 
    public_id, 
    'Policy is active and written or earned <= 0 ' AS errType 
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
    ) AS sum_of_type, 
    public_id, 
    'Policy is not active and written <> earned' AS errType 
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
    (SUM(monthly_written_premium) - SUM(monthly_earned_premium)) <> 0
) 
SELECT 
  public_id, 
  errType,
  sum_of_type
FROM 
  car_not_cancelled_monthly_written_premium 
UNION 
SELECT 
  public_id, 
  errType,
  sum_of_type
FROM 
  car_not_cancelled_monthly_earned_premium 
UNION
SELECT 
  public_id, 
  errType,
  sum_of_type
FROM 
  car_flat_cancelled_monthly_written_premium 
UNION 
SELECT 
  public_id, 
  errType,
  sum_of_type
FROM 
  car_flat_cancelled_monthly_earned_premium 
UNION 
SELECT 
  public_id, 
  errType,
  sum_of_type
FROM 
  monthly_unearned_premium 
UNION 
SELECT 
  public_id, 
  errType,
  sum_of_type
FROM 
  monthly_earned_premium 
UNION 
SELECT 
  public_id, 
  errType,
  sum_of_type
FROM 
  monthly_written_premium 
UNION 
SELECT 
  public_id, 
  errType,
  sum_of_type
FROM 
  car_active_policies_monthly_written_premium 
UNION 
SELECT 
  public_id, 
  errType,
  sum_of_type
FROM 
  car_active_policies_monthly_earned_premium 
UNION 
SELECT 
  public_id, 
  errType,
  sum_of_type 
FROM 
  car_inactive_policies_monthly_difference 
ORDER BY 
  errType
