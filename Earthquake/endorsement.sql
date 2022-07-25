with policy_versions AS (
select 
    b.policy_id AS policy_id,
    policy_version_id,
    premium_breakdown,
    TRY_PARSE_JSON(premium_breakdown) AS premium_breakdown_json, 
    premium_breakdown_json:endorsements AS endorsements
from monolith.policy_version_premiums a join monolith.policy_versions b on a.policy_version_id=b.id join monolith.policies c on b.policy_id=c.id
    )
,
policy_endorsements as(
select policy_id,
    policy_version_id,   
    value:type::string AS endorsement_type,
    value:value AS endorsement_value
    from policy_versions AS base,
        LATERAL FLATTEN(input => base.endorsements) AS endorsements
    WHERE endorsement_type = 'lemonade_earthquake'
)

--select count(*)--181,881
select policy_id,
policy_version_id,
sum(endorsement_value)
from policy_endorsements--181264
--where policy_id = 5227949 
group by policy_id,
policy_version_id

 
