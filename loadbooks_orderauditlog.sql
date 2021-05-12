with loadbooks_base as 
(
SELECT 
 oa.entityid Loadnum
,'LOADS BOOKED' Activity
,oa.entereddate ActivityDateTime
,date_trunc('MINUTE',oa.entereddate) ActivitySmallDateTime
,1 ActivityCount
,oa.enteredbypartycode Employee
,oa.sourcesystem ReasonCode
,oa.sourcesystemid SourceSystemID
,CASE WHEN oa.sourcesystemid in (480,481,2209,2210,3547,4194,5140,5326,5391) then 1 ELSE 0 END as IsAutomated
,1 as LoadBasedActivity

FROM ORION_RAP.OA.ORDERAUDIT as oa
where oa.entityid IN (348488385,349693769,349955162,356381848,356107724)
AND  oa.categoryid = 10
and oa.entitytypeid = 2 
and oa.applicationid = 5   ---5 = execution
and oa.actiontypeid = 9 
and oa.actionitemtypeid = 42
--and entereddate >= '2021-05-10'
--and actiontypeid = 7
AND trim(upper(oa.newvalue)) ='BOOKED'

GROUP BY 
oa.entityid 
,'LOADS BOOKED' 
,oa.entereddate 
,date_trunc('MINUTE',oa.entereddate) 
,oa.enteredbypartycode 
,oa.sourcesystem 
,oa.sourcesystemID
,CASE WHEN oa.sourcesystemid in (480,481,2209,2210,3547,4194,5140,5326,5391) then 1 
 ELSE null END
 )
 
 SELECT
 Loadnum
,Activity
,ActivityDateTime
,ActivitySmallDateTime
,ActivityCount
,Employee
,ReasonCode
,IsAutomated
,LoadBasedActivity
,row_number() over 
(partition by LoadNum,Employee,ActivitySmallDateTime
    order by LoadNum,Employee,ActivitySmallDateTime ASC) as BookRank

FROM loadbooks_base

GROUP BY
 Loadnum
,Activity
,ActivityDateTime
,ActivitySmallDateTime
,ActivityCount
,Employee
,ReasonCode
,IsAutomated
,LoadBasedActivity
 
