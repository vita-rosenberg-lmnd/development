select to_varchar(4714449)+to_varchar('other_members_of_household')
;
create or replace TABLE BI_DEVELOPMENT.dwh_endorsement_fact_written (
	ROW_ID NUMBER(38,0) autoincrement,
	policy_ID NUMBER(38,0),
	encrypted_policy_ID VARCHAR(100),
    policy_version_ID VARCHAR(100),
    endorsement_type VARCHAR(100),
    version_effective_date_from TIMESTAMP_NTZ(9),
    version_effective_date_to TIMESTAMP_NTZ(9),
    currency VARCHAR(10),
    change_amount FLOAT,
    amount FLOAT,
    annual FLOAT,
    ETL_version_from TIMESTAMP_NTZ(9),
    ETL_version_to TIMESTAMP_NTZ(9),
    etl_current_indicator NUMBER(38,0)
);


;
-------
MERGE INTO bi_development.dwh_endorsement_fact_written AS TARGET--new version, new endorsement for policy new or not
USING 
(SELECT 
 ROW_ID, POLICY_ID, ENCRYPTED_POLICY_ID, POLICY_VERSION_ID, ENDORSEMENT_TYPE, VERSION_EFFECTIVE_DATE_FROM, VERSION_EFFECTIVE_DATE_TO,   CURRENCY, ANNUAL_ENDORSEMENT_PREMIUM, VERSION_NUM_OF_DAYS, POLICY_LIFETIME_IN_DAYS, POLICY_EFFECTIVE_DATE, POLICY_RENEWAL_DATE, ENDORSMENT_PREMIUM, ETL_VERSION_FROM, ETL_VERSION_TO
FROM BI_DEVELOPMENT.STG_ENDORSEMENT_FACT_WRITTEN) AS source
on 
CONCAT(source.POLICY_ID,source.ENDORSEMENT_TYPE) = CONCAT(target.POLICY_ID,target.ENDORSEMENT_TYPE)
WHEN NOT MATCHED THEN 
INSERT(POLICY_ID, ENCRYPTED_POLICY_ID, POLICY_VERSION_ID, ENDORSEMENT_TYPE, VERSION_EFFECTIVE_DATE_FROM, 
 VERSION_EFFECTIVE_DATE_TO, CURRENCY, CHANGE_AMOUNT, AMOUNT, ANNUAL, 
 ETL_VERSION_FROM, ETL_VERSION_TO)
VALUES  (source.POLICY_ID
        ,source.ENCRYPTED_POLICY_ID
        ,source.POLICY_VERSION_ID
        ,source.ENDORSEMENT_TYPE
        ,source.VERSION_EFFECTIVE_DATE_FROM
        ,source.VERSION_EFFECTIVE_DATE_TO
        ,source.CURRENCY
        ,IFF(source.POLICY_LIFETIME_IN_DAYS = source.VERSION_NUM_OF_DAYS,0,source.ENDORSMENT_PREMIUM)
        ,source.ANNUAL_ENDORSEMENT_PREMIUM - source.ENDORSMENT_PREMIUM
        ,source.ANNUAL_ENDORSEMENT_PREMIUM
        ,CURRENT_DATE()
        ,NULL)
WHEN MATCHED AND
         target.POLICY_VERSION_ID <> source.POLICY_VERSION_ID
THEN UPDATE SET
         ETL_version_to = current_timestamp()
;



select * from bi_development.stg_endorsement_fact_written where policy_id=4180489
;
insert into bi_development.stg_endorsement_fact_written( POLICY_ID, ENCRYPTED_POLICY_ID, POLICY_VERSION_ID, ENDORSEMENT_TYPE, VERSION_EFFECTIVE_DATE_FROM, VERSION_EFFECTIVE_DATE_TO, CURRENCY, ANNUAL_ENDORSEMENT_PREMIUM, VERSION_NUM_OF_DAYS, POLICY_LIFETIME_IN_DAYS, POLICY_EFFECTIVE_DATE, POLICY_RENEWAL_DATE, ENDORSMENT_PREMIUM, ETL_VERSION_FROM, ETL_VERSION_TO, ETL_CURRENT_INDICATOR)

select POLICY_ID, ENCRYPTED_POLICY_ID, 214034356, ENDORSEMENT_TYPE, current_timestamp(), VERSION_EFFECTIVE_DATE_TO, CURRENCY, 30, 13, POLICY_LIFETIME_IN_DAYS, POLICY_EFFECTIVE_DATE, POLICY_RENEWAL_DATE, 1.0685, current_date(), ETL_VERSION_TO, 1
from bi_development.stg_endorsement_fact_written where POLICY_VERSION_ID=21403435 and endorsement_type='lemonade_earthquake'
;
update bi_development.stg_endorsement_fact_written
set etl_current_indicator = -1,
version_effective_date_to=current_timestamp(),
ETL_VERSION_TO = current_timestamp()
where POLICY_VERSION_ID = 21403435 and endorsement_type='lemonade_earthquake'
;
;select datediff(day,current_timestamp,'2022-10-01 07:01:00.000')--
;select 13*30/365
;
INSERT INTO bi_development.dwh_endorsement_fact_written--if endorsement is new and doesn't exist yet in dwh
(POLICY_ID, ENCRYPTED_POLICY_ID, POLICY_VERSION_ID, ENDORSEMENT_TYPE, VERSION_EFFECTIVE_DATE_FROM, 
 VERSION_EFFECTIVE_DATE_TO, CURRENCY, CHANGE_AMOUNT, AMOUNT, ANNUAL, 
 ETL_VERSION_FROM, ETL_VERSION_TO
)

SELECT 
 POLICY_ID, ENCRYPTED_POLICY_ID, POLICY_VERSION_ID, ENDORSEMENT_TYPE, VERSION_EFFECTIVE_DATE_FROM, 
 VERSION_EFFECTIVE_DATE_TO, CURRENCY, 0, ENDORSMENT_PREMIUM, ANNUAL_ENDORSEMENT_PREMIUM, 
 ETL_VERSION_FROM, ETL_VERSION_TO
FROM BI_DEVELOPMENT.STG_ENDORSEMENT_FACT_WRITTEN
WHERE etl_current_indicator = -1 
;

select * from bi_development.stg_endorsement_fact_written
;

;
    currency VARCHAR(10),
    change_amount FLOAT,
    amount FLOAT,
    annual FLOAT,
    ETL_version_from TIMESTAMP_NTZ(9),
    ETL_version_to TIMESTAMP_NTZ(9),
    etl_current_indicator NUMBER(38,0)







;
truncate table if exists BI_DEVELOPMENT.stg_endorsement_fact_written;
insert into BI_DEVELOPMENT.stg_endorsement_fact_written
(
POLICY_ID, ENCRYPTED_POLICY_ID, POLICY_VERSION_ID, ENDORSEMENT_TYPE, VERSION_EFFECTIVE_DATE_FROM, VERSION_EFFECTIVE_DATE_TO, CURRENCY, annual_endorsement_premium, 
VERSION_NUM_OF_DAYS, POLICY_LIFETIME_IN_DAYS, POLICY_EFFECTIVE_DATE, POLICY_RENEWAL_DATE, ENDORSMENT_PREMIUM, ETL_VERSION_FROM, ETL_VERSION_TO)

WITH policy_versions_cte AS (--json breakdown
select 
    pvp.policy_version_id,
    pvp.premium_breakdown,
    TRY_PARSE_JSON(premium_breakdown) AS premium_breakdown_json, 
    premium_breakdown_json:endorsements AS endorsements
from monolith.policy_version_premiums pvp
),

policy_endorsements_cte as(--json flatten and value extract
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
-- select distinct policy_id, DATE(convert_timezone('UTC', t.timezone, pa.effective_date)) AS event_date_local, 'update' AS event_type
-- from bi_development/*public*/.POLICY_PREMIUM_ACTIVITIES pa join monolith.policies p on pa.policy_id=p.id join timezones t on p.state=t.state_code
-- where activity='policy_update'
-- union
select distinct policy_id, DATE(convert_timezone('UTC', t.timezone, pa.effective_date)) AS event_date_local, 'cancel' AS event_type
from /*bi_development*/public.POLICY_PREMIUM_ACTIVITIES pa join monolith.policies p on pa.policy_id=p.id join timezones t on p.state=t.state_code
where activity='policy_cancelation'
-- union
-- select distinct policy_id, pa.effective_date AS event_date, 'reinstate' AS event_type
-- from bi_development/*public*/.POLICY_PREMIUM_ACTIVITIES pa
-- where activity='policy_reinstate'
-- union
-- select distinct policy_id, DATE(convert_timezone('UTC', t.timezone, pa.effective_date)) AS event_date_local, 'new_business' AS event_type
-- from /*bi_development*/public.POLICY_PREMIUM_ACTIVITIES pa join monolith.policies p on pa.policy_id=p.id join timezones t on p.state=t.state_code
-- where activity='new_business'
-- union
-- select distinct policy_id, DATE(convert_timezone('UTC', t.timezone, pa.effective_date)) AS event_date_local, 'new_business' AS event_type
-- from /*bi_development*/public.POLICY_PREMIUM_ACTIVITIES pa join monolith.policies p on pa.policy_id=p.id join timezones t on p.state=t.state_code
-- where activity='renewal'    
-- union    
-- select distinct policy_id,  date(convert_timezone('UTC', t.timezone, pa.effective_date)) AS event_date, 'new_business' AS event_type
-- from bi_development.POLICY_PREMIUM_ACTIVITIES pa join monolith.policies p on pa.policy_id=p.id join timezones t on p.state=t.state_code--public.policy_premium_activities a 
-- where activity='renewal'   
-- union    
-- select distinct policy_id,  date(convert_timezone('UTC', t.timezone, pa.effective_date)) AS event_date, 'update_effective_date' AS event_type
-- from bi_development.POLICY_PREMIUM_ACTIVITIES pa join monolith.policies p on pa.policy_id=p.id join timezones t on p.state=t.state_code--public.policy_premium_activities a 
-- where activity='policy_update_effective_date'--relevant only if policy already began       
),

calc_index_cte AS(--join to policy_versions
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
from policy_endorsements_cte a join monolith.policy_versions pv 
    on a.policy_version_id=pv.id
     join timezones t on pv.state=t.state_code
),

date_diff_cte AS (--remove extra rows for endorsement
select 
    p.encrypted_id AS encrypted_policy_ID,
    policy_id, 
    policy_version_id, 
    ROW_NUMBER() OVER (PARTITION BY policy_id, encrypted_id, DATE(start_date_local), endorsement_type ORDER BY policy_version_id DESC) AS Last_version_a_day,
    c.annual_premium, 
    p.premium AS policy_premium,
    endorsement_type,
    start_date AS version_effective_date_from,
    coalesce(end_date, p.renewal_date) AS version_effective_date_to,
    p.effective_date AS policy_effective_date,
    p.renewal_date AS policy_renewal_date
from calc_index_cte c JOIN monolith.policies p on c.policy_id=p.id
     WHERE row_number_of_calc_index = 1
),

one_version_per_day AS(--bring only one version a day
select DENSE_RANK() OVER (PARTITION BY policy_id ORDER by policy_version_id) AS number_of_version_per_policy,    
       encrypted_policy_ID,
       policy_id, 
       policy_version_id,
       annual_premium,
       policy_premium,
       endorsement_type,
       version_effective_date_from,
       version_effective_date_to, 
       policy_effective_date,
       policy_renewal_date,
       datediff(day,policy_effective_date, policy_renewal_date) policy_lifetime_in_days,
       datediff(day,version_effective_date_from, version_effective_date_to) version_num_of_days
from  date_diff_cte a
where Last_version_a_day = 1 AND version_effective_date_from < version_effective_date_to
)

select a.policy_id, encrypted_policy_ID, policy_version_id, endorsement_type, version_effective_date_from, version_effective_date_to, 'USD' AS CURRENCY, 
annual_premium, version_num_of_days, policy_lifetime_in_days, policy_effective_date, policy_renewal_date,
round(version_num_of_days/policy_lifetime_in_days*annual_premium, 4) AS endorsment_premium,
--coalesce(pea.event_type, 'update') AS event_type, 
-- peb.event_type, 
--pea.event_date_local AS cancel_date,
-- peb.event_date_local AS update_date,
CURRENT_DATE() AS ETL_VERSION_FROM,  
NULL AS ETL_VERSION_TO
from one_version_per_day a 
 -- left join policy_events pea on a.policy_id=pea.policy_id AND 
order by version_effective_date_from
;--remove policie_versions that were started and ended in the same day
POLICY_ID, ENCRYPTED_POLICY_ID, POLICY_VERSION_ID, ENDORSEMENT_TYPE, VERSION_EFFECTIVE_DATE_FROM, VERSION_EFFECTIVE_DATE_TO, CURRENCY, annual_endorsement_premium, 
VERSION_NUM_OF_DAYS, POLICY_LIFETIME_IN_DAYS, POLICY_EFFECTIVE_DATE, POLICY_RENEWAL_DATE, ENDORSMENT_PREMIUM, ETL_VERSION_FROM, ETL_VERSION_TO
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

select policy_version_id, count(policy_version_id), endorsement_type
from BI_DEVELOPMENT.stg_endorsement_fact_written
where endorsement_type='lemonade_earthquake'
group by policy_version_id, endorsement_type
having count(policy_version_id)>1
order by count(policy_version_id) desc
-- where policy_id = 5380133 
;
with relevant AS(
select policy_id from monolith.policy_version_premiums a join monolith.policy_versions b on  a.policy_version_id=b.id
where premium_breakdown like '%endorsement%')

select * from bi_development.POLICY_PREMIUM_ACTIVITIES--filter on US
       where policy_id =6030897
       ;
select * from                  
BI_DEVELOPMENT.stg_endorsement_fact_written
where --policy_id = 4798666  
policy_version_id=26645916
and endorsement_type='scheduled_personal_property'
order by version_effective_date_from
;
select *
from                  
BI_DEVELOPMENT.stg_endorsement_fact_written
where 
policy_version_id=20209236 
--policy_id=3890304
-- and endorsement_type='lemonade_earthquake'
order by version_effective_date_from 
;

select *
from                  
BI_DEVELOPMENT.stg_endorsement_fact_written
where endorsement_type='lemonade_earthquake'
;
select * --2021-09-17 07:01:00.000 effective date of ne_business
from public.POLICY_PREMIUM_ACTIVITIES
where policy_id=1000119
;
select * from monolith.policy_versions
where policy_id=1000119
;
select *
from BI_DEVELOPMENT.stg_endorsement_fact_written
where 
policy_id=1000119
-- and endorsement_type='lemonade_earthquake'
order by policy_version_id, version_effective_date_from 
;

WITH sanity AS(
select policy_id, count(policy_version_id), endorsement_type
from BI_DEVELOPMENT.stg_endorsement_fact_written 
    where endorsement_type is not null
group by policy_id, endorsement_type
having count(policy_id)>2
-- order by count(policy_version_id) desc
    )
select * from     public.POLICY_PREMIUM_ACTIVITIES
where policy_id in (select policy_id from sanity) 
order by policy_id, effective_date
;

select * from monolith.policies
where id=1013506
;
select * from     public.POLICY_PREMIUM_ACTIVITIES
where policy_id=1000119
;
select *
from monolith.policy_versions
where policy_id=16678
order by start_date
;
select count(*) from(--4,059,062--4,059,064
select distinct policy_id, DATE(convert_timezone('UTC', t.timezone, pa.effective_date)) AS event_date_local, 'new_business' AS event_type
from /*bi_development*/public.POLICY_PREMIUM_ACTIVITIES pa join monolith.policies p on pa.policy_id=p.id join timezones t on p.state=t.state_code
where activity='new_business' and p.country = 'US'
    )
;
