;
create or replace TABLE LEMONADE_DEVELOPMENT.BI_DEVELOPMENT.FACT_WRITTEN (
	ROW_ID NUMBER(38,0) autoincrement,
	policy_ID NUMBER(38,0),
	encrypted_policy_ID VARCHAR(100),
    policy_version_ID VARCHAR(100),
    endorsement_type VARCHAR(100),
    effective_date_from TIMESTAMP_NTZ(9),
    effective_date_to TIMESTAMP_NTZ(9),
    currency VARCHAR(10),
    change_value NUMBER(38,0),
    value NUMBER(38,0),
    ETL_version_from TIMESTAMP_NTZ(9),
    ETL_version_to TIMESTAMP_NTZ(9)
);


insert into LEMONADE_DEVELOPMENT.BI_DEVELOPMENT.FACT_WRITTEN
(
policy_id, ENCRYPTED_POLICY_ID, policy_version_id, endorsement_type, ETL_VERSION_FROM, ETL_VERSION_TO, EFFECTIVE_DATE_FROM, EFFECTIVE_DATE_TO, CURRENCY, CHANGE_VALUE, VALUE
)

WITH policy_versions_cte AS (
select 
    pvp.policy_version_id,
    pvp.premium_breakdown,
    TRY_PARSE_JSON(premium_breakdown) AS premium_breakdown_json, 
    premium_breakdown_json:endorsements AS endorsements
from monolith.policy_version_premiums pvp
),
    
policy_endorsements_cte as(
select 
    policy_version_id,   
    value:type::string AS endorsement_type,
    value:value AS endorsement_value,
    value:calc_index AS calc_index
    from policy_versions_cte AS base,
        LATERAL FLATTEN(input => base.endorsements) AS endorsements
    --WHERE endorsement_type = 'lemonade_earthquake'
),

calc_index_cte AS(
select
    pv.policy_id,
    policy_version_id,   
    pv.start_date,
    LAST_DAY(pv.end_date) AS end_of_month_date,
    convert_timezone('UTC', t.timezone, pv.start_date) AS start_date_local,
    endorsement_value as value,
    ROW_NUMBER() OVER (PARTITION BY policy_version_id, endorsement_type ORDER BY calc_index DESC) AS row_number_of_calc_index,
    endorsement_type
from policy_endorsements_cte a join monolith.policy_versions pv on a.policy_version_id=pv.id
     join timezones t on pv.state=t.state_code
),

date_diff_cte AS (
select 
    p.encrypted_id AS encrypted_policy_ID,
    policy_id, 
    policy_version_id, 
    ROW_NUMBER() OVER (PARTITION BY policy_id, DATE(start_date_local), endorsement_type ORDER BY policy_version_id DESC) AS Last_version_a_day,
    value, 
    endorsement_type,
    start_date AS effective_date_from,
    coalesce(end_of_month_date, p.renewal_date) AS effective_date_to    
from calc_index_cte c JOIN monolith.policies p on c.policy_id=p.id
     WHERE row_number_of_calc_index = 1
),
    
one_version_per_day AS(
select DENSE_RANK() OVER (PARTITION BY policy_id ORDER by policy_version_id) AS number_of_version_per_policy,    
       encrypted_policy_ID,
       policy_id, 
       policy_version_id,
       value,
       value AS change_value,
       endorsement_type,
       effective_date_from,
       effective_date_to
from  date_diff_cte
where Last_version_a_day = 1
),

cte_first_version AS(
select 
       CASE WHEN number_of_version_per_policy = 1 THEN value else 0 end as value,
       encrypted_policy_ID,
       policy_id, 
       policy_version_id,
       change_value,
       endorsement_type,
       effective_date_from,
       effective_date_to
    from one_version_per_day
)

select policy_id, ENCRYPTED_POLICY_ID, policy_version_id, endorsement_type, 
CURRENT_DATE(), NULL, EFFECTIVE_DATE_FROM, EFFECTIVE_DATE_TO, 'USD', CHANGE_VALUE, VALUE 
from cte_first_version
where policy_version_id not in (select policy_version_id from BI_DEVELOPMENT.FACT_WRITTEN)
-- 
;
with 
relevant AS (
SELECT POLICY_ID FROM LEMONADE.PUBLIC.POLICY_PREMIUM_ACTIVITIES
WHERE ACTIVITY = 'new_business'
AND POLICY_ID NOT IN (SELECT POLICY_ID FROM LEMONADE.PUBLIC.POLICY_PREMIUM_ACTIVITIES 
                     WHERE ACTIVITY IN (
                         'policy_update_effective_date'
                        ,'policy_cancelation'
                        ,'policy_update'
                        ,'data_correction'
                        ,'renewal'
                        ,'policy_reinstate'
                        ,'policy_premium_adjustment')
                     )
)

select sum(value) s, policy_id
from BI_DEVELOPMENT.FACT_WRITTEN
where policy_id in
(select policy_id from relevant)
and endorsement_type='lemonade_earthquake'
and effective_date_from > '2021-01-01'
group by policy_id
order by policy_id
;
select * 
from BI_DEVELOPMENT.FACT_WRITTEN
order by effective_date_from
;
policy_id, ENCRYPTED_POLICY_ID, policy_version_id, endorsement_type, ROW_VERSION_FROM, ROW_VERSION_TO, EFFECTIVE_DATE_FROM, EFFECTIVE_DATE_TO, CURRENCY, CHANGE_VALUE, VALUE


select * from cte_first_version
where policy_id = 4306259
and endorsement_type='lemonade_earthquake'
-- order by number_of_version_per_policy
;
select * from public.earthquake_report where policy_id = 4306259 order by month, policy_version_id limit 100;


;

-- MERGE INTO bi_development.fact_written AS TARGET
--     USING (
WITH policy_versions_cte AS (
select 
    pvp.policy_version_id,
    pvp.premium_breakdown,
    TRY_PARSE_JSON(premium_breakdown) AS premium_breakdown_json, 
    premium_breakdown_json:endorsements AS endorsements
from monolith.policy_version_premiums pvp
),
    
policy_endorsements_cte as(
select 
    policy_version_id,   
    value:type::string AS endorsement_type,
    value:value AS endorsement_value,
    value:calc_index AS calc_index
    from policy_versions_cte AS base,
        LATERAL FLATTEN(input => base.endorsements) AS endorsements
    --WHERE endorsement_type = 'lemonade_earthquake'
),

calc_index_cte AS(
select
    pv.policy_id,
    policy_version_id,   
    pv.start_date,
    LAST_DAY(pv.end_date) AS end_of_month_date,
    convert_timezone('UTC', t.timezone, pv.start_date) AS start_date_local,
    endorsement_value as value,
    ROW_NUMBER() OVER (PARTITION BY policy_version_id, endorsement_type ORDER BY calc_index DESC) AS row_number_of_calc_index,
    endorsement_type
from policy_endorsements_cte a join monolith.policy_versions pv on a.policy_version_id=pv.id
     join timezones t on pv.state=t.state_code
),

date_diff_cte AS (
select 
    p.encrypted_id AS encrypted_policy_ID,
    policy_id, 
    policy_version_id, 
    ROW_NUMBER() OVER (PARTITION BY policy_id, DATE(start_date_local), endorsement_type ORDER BY policy_version_id DESC) AS Last_version_a_day,
    value, 
    endorsement_type,
    start_date AS effective_date_from,
    coalesce(end_of_month_date, p.renewal_date) AS effective_date_to    
from calc_index_cte c JOIN monolith.policies p on c.policy_id=p.id
     WHERE row_number_of_calc_index = 1
),
    
one_version_per_day AS(
select DENSE_RANK() OVER (PARTITION BY policy_id ORDER by policy_version_id) AS number_of_version_per_policy,    
       encrypted_policy_ID,
       policy_id, 
       policy_version_id,
       value,
       value AS change_value,
       endorsement_type,
       effective_date_from,
       effective_date_to
from  date_diff_cte
where Last_version_a_day = 1
),

cte_first_version AS(
select 
       CASE WHEN number_of_version_per_policy = 1 THEN value else 0 end as value,
       encrypted_policy_ID,
       policy_id, 
       policy_version_id,
       change_value,
       endorsement_type,
       effective_date_from,
       effective_date_to
    from one_version_per_day
),
; 
with 
        relevant AS (
    SELECT POLICY_ID FROM LEMONADE.PUBLIC.POLICY_PREMIUM_ACTIVITIES
        WHERE ACTIVITY = 'new_business'
        AND POLICY_ID NOT IN (SELECT POLICY_ID FROM LEMONADE.PUBLIC.POLICY_PREMIUM_ACTIVITIES 
                             WHERE ACTIVITY IN (
                                 'policy_update_effective_date'
                                ,'policy_cancelation'
                                ,'policy_update'
                                ,'data_correction'
                                ,'renewal'
                                ,'policy_reinstate'
                                ,'policy_premium_adjustment')
                             )
        )
        select policy_id from relevant
        where policy_id=5227949
        ;
select sum(endorsement_written_premium), policy_id 
from public.earthquake_report 
where policy_id in()
group by policy_id
order by month, policy_version_id limit 100;
        
        ;
        
        
        
 ;       
 select * 
 from LEMONADE.PUBLIC.POLICY_PREMIUM_ACTIVITIES
 where policy_id=5227949
 order by effective_date
        ;
        


-- select * from old_report
-- order by s desc

    select sum(value), policy_id
    from cte_first_version
    where policy_id in (select POLICY_ID from relevant)
    and 
    policy_id in (select POLICY_ID from old_report)
    and endorsement_type='lemonade_earthquake'
    group by policy_id
    order by sum(value) desc

        ;
        
SELECT DISTINCT ACTIVITY FROM 
LEMONADE.PUBLIC.POLICY_PREMIUM_ACTIVITIES        
        ;
with old_report AS 
(select sum(endorsement_written_premium) as s ,policy_id --960,130.5204
from public.earthquake_report 
where policy_id in (select POLICY_ID from relevant)
group by policy_id)
       ;
       
with relevant AS (SELECT POLICY_ID 
FROM LEMONADE.PUBLIC.POLICY_PREMIUM_ACTIVITIES 
                     WHERE ACTIVITY IN (
                        ,'policy_update_effective_date'
                        ,'policy_cancelation'
                        ,'policy_update'
                        ,'data_correction'
                        ,'renewal'
                        ,'policy_reinstate'
                        ,'policy_premium_adjustment')
                  )
  select sum(endorsement_written_premium) as s ,policy_id --960,130.5204
from public.earthquake_report 
where policy_id in (select POLICY_ID from relevant)
group by policy_id                
                  
;

select * from
public.policy_endorsements
where policy_id=5227949
;
