--this is saved here to show that there is no record for a policy version id in table monolith.policy_version_premiums if the version doesn't have an endorsement
create or replace TABLE LEMONADE_DEVELOPMENT.BI_DEVELOPMENT.stg_endorsement_fact_written (
	ROW_ID NUMBER(38,0) autoincrement,
	policy_ID NUMBER(38,0),
	encrypted_policy_ID VARCHAR(100),
    policy_version_ID VARCHAR(100),
    endorsement_type VARCHAR(100),
    version_effective_date_from TIMESTAMP_NTZ(9),
    version_effective_date_to TIMESTAMP_NTZ(9),
    currency VARCHAR(10),
    -- value NUMBER(38,0),
    annual_premium NUMBER(38,0),
    version_num_of_days NUMBER(38,0), 
    policy_lifetime_in_days NUMBER(38,0),
    policy_effective_date TIMESTAMP_NTZ(9),
    policy_renewal_date TIMESTAMP_NTZ(9),
    endorsment_premium FLOAT,
    event_type VARCHAR(100), 
    event_date VARCHAR(100),
    ETL_version_from TIMESTAMP_NTZ(9),
    ETL_version_to TIMESTAMP_NTZ(9)
);

truncate table if exists BI_DEVELOPMENT.stg_endorsement_fact_written;
insert into LEMONADE_DEVELOPMENT.BI_DEVELOPMENT.stg_endorsement_fact_written
(
POLICY_ID, ENCRYPTED_POLICY_ID, POLICY_VERSION_ID, ENDORSEMENT_TYPE, VERSION_EFFECTIVE_DATE_FROM, VERSION_EFFECTIVE_DATE_TO, CURRENCY, ANNUAL_PREMIUM, VERSION_NUM_OF_DAYS, POLICY_LIFETIME_IN_DAYS, POLICY_EFFECTIVE_DATE, POLICY_RENEWAL_DATE, ENDORSMENT_PREMIUM, EVENT_TYPE, EVENT_DATE, ETL_VERSION_FROM, ETL_VERSION_TO)
;
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

policy_events AS(
select distinct policy_id, DATE(convert_timezone('UTC', t.timezone, pa.effective_date)) AS event_date_local, 'update' AS event_type
from bi_development/*public*/.POLICY_PREMIUM_ACTIVITIES pa join monolith.policies p on pa.policy_id=p.id join timezones t on p.state=t.state_code
where activity='policy_update'
union
select distinct policy_id, DATE(convert_timezone('UTC', t.timezone, pa.effective_date)) AS event_date_local, 'cancel' AS event_type
from bi_development/*public*/.POLICY_PREMIUM_ACTIVITIES pa join monolith.policies p on pa.policy_id=p.id join timezones t on p.state=t.state_code
where activity='policy_cancelation'
-- union
-- select distinct policy_id, pa.effective_date AS event_date, 'reinstate' AS event_type
-- from bi_development/*public*/.POLICY_PREMIUM_ACTIVITIES pa
-- where activity='policy_reinstate'
union
select distinct policy_id, pa.effective_date AS event_date, 'new_business' AS event_type
from bi_development/*public*/.POLICY_PREMIUM_ACTIVITIES pa
where activity='new_business'
union
select distinct policy_id, pa.effective_date AS event_date, 'new_business' AS event_type
from bi_development/*public*/.POLICY_PREMIUM_ACTIVITIES pa
where activity='renewal'    
-- union    
-- select distinct policy_id,  date(convert_timezone('UTC', t.timezone, pa.effective_date)) AS event_date, 'new_business' AS event_type
-- from bi_development.POLICY_PREMIUM_ACTIVITIES pa join monolith.policies p on pa.policy_id=p.id join timezones t on p.state=t.state_code--public.policy_premium_activities a 
-- where activity='renewal'   
),

calc_index_cte AS(
select
    pv.policy_id,
    pv.id AS policy_version_id,   
    pv.start_date,    
    pv.end_date,
    convert_timezone('UTC', t.timezone, pv.start_date) AS start_date_local,
    convert_timezone('UTC', t.timezone, pv.end_date) AS end_date_local,    
    endorsement_value as annual_premium,
    ROW_NUMBER() OVER (PARTITION BY pv.id, endorsement_type ORDER BY calc_index DESC) AS row_number_of_calc_index,
    endorsement_type
from policy_endorsements_cte a right join monolith.policy_versions pv 
    on a.policy_version_id=pv.id
     join timezones t on pv.state=t.state_code
)

select * from calc_index_cte
where policy_id=4372274
;
,

date_diff_cte AS (
select 
    p.encrypted_id AS encrypted_policy_ID,
    policy_id, 
    policy_version_id, 
    ROW_NUMBER() OVER (PARTITION BY policy_id, encrypted_id, DATE(start_date_local), endorsement_type ORDER BY policy_version_id DESC) AS Last_version_a_day,
    c.annual_premium, 
    endorsement_type,
    start_date AS version_effective_date_from,
    coalesce(end_date, p.renewal_date) AS version_effective_date_to,
    p.effective_date AS policy_effective_date,
    p.renewal_date AS policy_renewal_date
from calc_index_cte c JOIN monolith.policies p on c.policy_id=p.id
     WHERE row_number_of_calc_index = 1
)
select * from date_diff_cte
where policy_id=4372274
;

select * from policy_events a join date_diff_cte b 
on a.policy_id=b.policy_id AND-- date(b.version_effective_date_to) = date(a.event_date_local)
IFF(a.event_type='cancel',date(b.version_effective_date_to) = date(a.event_date_local),
date(b.version_effective_date_from) = date(a.event_date_local))
where a.policy_id=4372274 or b.policy_id = 4372274
;
,

one_version_per_day AS(
select DENSE_RANK() OVER (PARTITION BY policy_id ORDER by policy_version_id) AS number_of_version_per_policy,    
       encrypted_policy_ID,
       policy_id, 
       policy_version_id,
       annual_premium,
       endorsement_type,
       version_effective_date_from,
       version_effective_date_to, 
       policy_effective_date,
       policy_renewal_date,
       datediff(day,policy_effective_date, policy_renewal_date) policy_lifetime_in_days,
       datediff(day,version_effective_date_from, version_effective_date_to) version_num_of_days
from  date_diff_cte a
where Last_version_a_day = 1
)

select a.policy_id, encrypted_policy_ID, policy_version_id, endorsement_type, version_effective_date_from, version_effective_date_to, 'USD' AS CURRENCY, 
annual_premium, version_num_of_days, 
policy_lifetime_in_days,
policy_effective_date,
policy_renewal_date,
round(version_num_of_days/policy_lifetime_in_days*annual_premium, 4) AS endorsment_premium,
pea.event_type, 
peb.event_type, 
pea.event_date_local AS cancel_date,
peb.event_date_local AS update_date,
CURRENT_DATE() AS ETL_VERSION_FROM,  
NULL AS ETL_VERSION_TO
from one_version_per_day a left join policy_events pea on a.policy_id=pea.policy_id AND 
pea.event_type = 'cancel'-- AND date(a.version_effective_date_to) = date(pea.event_date_local)
left join policy_events peb on a.policy_id=peb.policy_id --AND 
-- date(a.version_effective_date_from) = date(peb.event_date_local) 
AND peb.event_type = 'update'
-- date(a.version_effective_date_to) = date(pe.event_date_local)
-- where a.policy_id = 5251420--1,528,926--1,555,488
where a.policy_id=4372274
order by version_effective_date_from
;--remove policie_versions that were started and ended in the same day


select * from monoloth


;
select date(current_date())
;
select sum(endorsment_premium), count(endorsment_premium),
policy_id
from BI_DEVELOPMENT.stg_endorsement_fact_written
group by policy_id
having count(endorsment_premium)>1
-- where annual_premium<>endorsment_premium
-- and policy_id=3917747
-- where event_date is null
;

select * from 
bi_development.POLICY_PREMIUM_ACTIVITIES
where policy_id =  5251420
;
select * from 
bi_development.POLICY_PREMIUM_ACTIVITIES
where activity = 'data_correction'
;

select * from 
bi_development.POLICY_PREMIUM_ACTIVITIES
where policy_id = 4374173
;

select * from monolith.policy_versions
where policy_id = 4374173 
;

select policy_id, endorsement_type, count(policy_id)
from BI_DEVELOPMENT.stg_endorsement_fact_written
where endorsement_type='lemonade_earthquake'
group by policy_id, endorsement_type
having count(policy_id)>1
-- where policy_id = 5380133 
;
with relevant AS(
select policy_id from monolith.policy_version_premiums a join monolith.policy_versions b on  a.policy_version_id=b.id
where premium_breakdown like '%endorsement%')

select * from bi_development.POLICY_PREMIUM_ACTIVITIES
       where policy_id =6030897
       ;
select * from                  
BI_DEVELOPMENT.stg_endorsement_fact_written
where policy_id = 4372274 --and endorsement_type='lemonade_earthquake'
order by version_effective_date_from
;
select *
from                  
BI_DEVELOPMENT.stg_endorsement_fact_written
where 
-- endorsment_premium > 50 and endorsment_premium <> annual_premium 
-- and 
policy_id=4700542
and endorsement_type='lemonade_earthquake'
order by version_effective_date_from 
;
select * from bi_development.POLICY_PREMIUM_ACTIVITIES 
where policy_id = 4700542
;
select * from monolith.policy_version_premiums
where policy_version_id in (
18301888,18464216
)
--and premium_breakdown like '%endorsement%'

;
select a.policy_id,state
from bi_development.POLICY_PREMIUM_ACTIVITIES a join monolith.policies b on a.policy_id=b.id

;
select * --2021-09-17 07:01:00.000 effective date of ne_business
from bi_development.POLICY_PREMIUM_ACTIVITIES
where policy_id=4372274
;
select * from monolith.policy_versions
where policy_id=4372274
;


select *
from BI_DEVELOPMENT.stg_endorsement_fact_written
where version_effective_date_to > current_date()+2 --policy_id=4700542
and endorsement_type='lemonade_earthquake' and endorsment_premium<>annual_premium
order by version_effective_date_from 
;
select * from bi_development.POLICY_PREMIUM_ACTIVITIES 
where policy_id = 4700542
