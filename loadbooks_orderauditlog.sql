SELECT
 oa.entityid Loadnum
,'LOADS BOOKED' Activity
,oa.entereddate ActivityDateTime
,date_trunc('MINUTE',oa.entereddate) ActivitySmallDateTime
,1 ActivityCount
,oa.enteredbypartycode Employee
,oa.sourcesystem ReasonCode
,oa.sourcesystemid SourceSystemID
,CASE WHEN oa.sourcesystemid in (480,481,2209,2210,3547,4194,5140,5326,5391) then 1
    WHEN BOT.AUTOMATED = 'AUTO' THEN 1 ELSE 0 END as IsAutomated
,1 as LoadBasedActivity
                ,O.CUSTOMERID  AS CUSTOMERID
                ,O.CUSTOMERCODE  AS CUSTOMERCODE
                ,O.SERVICEOFFERINGDESC AS MODE
                ,O.CUSTOMERBRANCHID AS BRANCHID
                ,O.CUSTOMERBRANCHCODE AS BRANCHCODE
                ,EMPBRANCH.PARTYID AS EMPLOYEEBRANCHID
                ,EL.BRANCHCODE AS EMPLOYEEBRANCHCODE
                ,UPPER(TRIM(EL.PSROLE)) AS EMPLOYEEPOSITION
                ,CASE
                    WHEN oa.sourcesystemid in (480,481,2209,2210,3547,4194,5140,5326,5391) then 1
                    WHEN (BOT.AUTOMATED = 'AUTO'
                        OR E.EMPCODE IS NULL
                        OR E.BRANCHCODE = '7650') THEN 1
                    ELSE 0
                END AS ISAUTOMATED
                ,IFF(BOT.AUTOMATED = 'AUTO' ,1 ,0) AS ISBOT

FROM ORION_RAP.OA.ORDERAUDIT as oa
 LEFT OUTER JOIN ORION_RAP.EP.ACTIVITY as A on a.loadnumber = oa.entityID
 LEFT OUTER JOIN OPERATIONS_DOMAIN.DBO.ORDER_CHARACTERISTICS O ON
                    a.ORDERNUMBER = O.ORDERNUM
    LEFT OUTER JOIN EXPRESS_RAP.DBO.EMPLOYEES E  ON
                    Upper(TRIM(oa.enteredbypartycode)) = TRIM(E.EMPCODE)
                LEFT OUTER JOIN TRUCKLOAD_DOMAIN.DBO.DIM_EMPLOYEE_HISTORY EL ON
                    TRIM(EL.EMPLOYEECODE) = TRIM(E.EMPCODE)
                    AND oa.entereddate >= EL.ACTIVESTARTDATETIME
                    AND oa.entereddate <= EL.ACTIVEENDDATETIME
                LEFT JOIN MDM_RAP.MDM.PARTY EMPBRANCH ON
                    TRIM(EL.BRANCHCODE) = EMPBRANCH.PARTYCODE
                    AND EMPBRANCH.PARTYTYPEID = 1 /*BRANCH*/
    LEFT OUTER JOIN OPERATIONS_DOMAIN.DBO.OPEX_BOTS BOT
    ON Upper(TRIM(oa.enteredbypartycode)) = BOT.EmpCode

WHERE 
---oa.entityid IN (348488385,349693769,349955162,356381848,356107724) AND  
oa.categoryid = 10
AND oa.entitytypeid = 2
AND oa.applicationid = 5   ---5 = execution
AND oa.actiontypeid = 9
AND oa.actionitemtypeid = 42
and oa.entereddate >= '2021-05-10'
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
,CASE 
    WHEN oa.sourcesystemid in (480,481,2209,2210,3547,4194,5140,5326,5391) then 1
    WHEN BOT.AUTOMATED = 'AUTO' THEN 1 ELSE 0 END
,O.CUSTOMERID
                ,O.CUSTOMERCODE
                ,O.SERVICEOFFERINGDESC
                ,O.CUSTOMERBRANCHID
                ,O.CUSTOMERBRANCHCODE
                ,EMPBRANCH.PARTYID
                ,EL.BRANCHCODE 
                ,UPPER(TRIM(EL.PSROLE)) 
                ,CASE
                    WHEN oa.sourcesystemid in (480,481,2209,2210,3547,4194,5140,5326,5391) then 1
                    WHEN (BOT.AUTOMATED = 'AUTO'
                        OR E.EMPCODE IS NULL
                        OR E.BRANCHCODE = '7650') THEN 1
                    ELSE 0
                END 
                ,IFF(BOT.AUTOMATED = 'AUTO' ,1 ,0)
 
     QUALIFY row_number() over (PARTITION BY LoadNum,Employee,ActivitySmallDateTime 
                                    ORDER BY LoadNum,Employee,ActivitySmallDateTime ASC) = 1
