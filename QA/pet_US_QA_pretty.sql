WITH temp_premium_report_us AS(
  select 
    * 
  FROM 
    pet_finance.premium_report_us
), 

pet_not_cancelled AS(
  SELECT 
    public_id 
  FROM 
    pet.policies 
  WHERE 
    canceled_at IS NULL
), 

pet_not_cancelled_monthly_written_premium AS(
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
        pet_not_cancelled
    ) 
  GROUP BY 
    public_id 
  HAVING 
    SUM(monthly_written_premium) = 0
), 

pet_not_cancelled_monthly_earned_premium AS(
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
        pet_not_cancelled
    ) 
  GROUP BY 
    public_id 
  HAVING 
    SUM(monthly_earned_premium) = 0
), 

--pet Flat canclled with written or earned <> 0
pet_flat_cancelled AS(
  SELECT 
    entity_id AS policyId 
  FROM 
    billing.finance_events activity 
  WHERE 
    activity = 'policy_cancellation' 
    AND metadata : flat = 'true' 
    AND entity_id LIKE 'LPP%'
), 

pet_flat_cancelled_monthly_written_premium AS(
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
        pet_flat_cancelled
    ) 
  GROUP BY 
    public_id 
  HAVING 
    ROUND(SUM(monthly_written_premium), 4) <> 0
), 

pet_flat_cancelled_monthly_earned_premium AS(
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
        pet_flat_cancelled
    ) 
  GROUP BY 
    public_id 
  HAVING 
        ROUND(SUM(monthly_earned_premium), 4) <> 0
), 

--monthly_unearned_premium < 0
monthly_unearned_premium AS(
  SELECT 
    SUM(monthly_unearned_premium) AS sum_of_type, 
    public_id, 
    'Monthly unearned premium < 0' AS errType 
  FROM 
    temp_premium_report_us 
  GROUP BY 
    public_id 
  HAVING 
    ROUND(SUM(monthly_unearned_premium), 4) < 0
), 

--monthly_earned_premium < 0
monthly_earned_premium AS(
  SELECT 
    SUM(monthly_earned_premium) AS sum_of_type, 
    public_id, 
    'Monthly earned premium < 0' AS errType 
  FROM 
    temp_premium_report_us 
  GROUP BY 
    public_id 
  HAVING 
    ROUND(SUM(monthly_earned_premium), 4) < 0
), 

--monthly_written_premium < 0
monthly_written_premium AS(
  SELECT 
    SUM(monthly_written_premium) AS sum_of_type, 
    public_id, 
    'Monthly written premium < 0' AS errType 
  FROM 
    temp_premium_report_us 
  GROUP BY 
    public_id 
  HAVING 
    ROUND(SUM(monthly_written_premium), 4) < 0
), 

--pet Policy is active and written or earned <= 0
pet_active_policies AS(
  SELECT 
    public_id 
  FROM 
    pet.policies 
  WHERE 
    status = 'active'
), 

pet_active_policies_monthly_written_premium AS(
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
        pet_active_policies
    ) 
  GROUP BY 
    public_id 
  HAVING 
    SUM(monthly_written_premium) <= 0
), 

pet_active_policies_monthly_earned_premium AS(
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
        pet_active_policies
    ) 
  GROUP BY 
    public_id 
  HAVING 
    ROUND(SUM(monthly_earned_premium), 4) <= 0
),

--pet Policy is not active and written <> earned
pet_inactive_policies AS(
  SELECT 
    public_id 
  FROM 
    pet.policies 
  WHERE 
    status <> 'active'
), 

pet_inactive_policies_monthly_difference AS(
  SELECT 
    (
      ROUND((SUM(monthly_written_premium) - SUM(monthly_earned_premium)), 4)
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
        pet_inactive_policies
    ) 
  GROUP BY 
    public_id 
  HAVING 
    ROUND((SUM(monthly_written_premium) - SUM(monthly_earned_premium)), 4) <> 0
) 
SELECT 
  public_id, 
  errType
FROM 
  pet_not_cancelled_monthly_written_premium 
UNION 
SELECT 
  public_id, 
  errType
FROM 
  pet_not_cancelled_monthly_earned_premium 
UNION
SELECT 
  public_id, 
  errType
FROM 
  pet_flat_cancelled_monthly_written_premium 
UNION 
SELECT 
  public_id, 
  errType
FROM 
  pet_flat_cancelled_monthly_earned_premium 
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
  pet_active_policies_monthly_written_premium 
UNION 
SELECT 
  public_id, 
  errType
FROM 
  pet_active_policies_monthly_earned_premium 
UNION 
SELECT 
  public_id, 
  errType
FROM 
  pet_inactive_policies_monthly_difference 
ORDER BY 
  errType
