With

Shipment_Activity as -- Getting current shipment activity  Events Updated records for Q1
(
Select 
a.ACTIVITY
,a.LOADNUM
,a.ACTIVITYDATETIME
,a.EMPLOYEE
,a.activitycount
,a.activitycycletimeduration
,a.moderollup
,a.employeebranchID 
,a.customerid
,a.employeeposition
,a.reasoncode
 
FROM OPERATIONS_DOMAIN.DBO.SHIPMENT_ACTIVITY a

WHERE   --a.Loadnum in ( '353895523','342524810') -- for testing
        a.ACTIVITY in ('EVENTS UPDATED')
      and a.ActivityDateTime >= '2021-01-01'
      AND a.ActivityDateTime  < '2021-04-01'
      AND a.ISAUTOMATED = 'FALSE'
  )
  
  
  
  
,TimeZoneAdjustment as -- Building out Branch Time zone

(Select
BA.BRANCHNAME
,BA.BRANCHCODE
,BA.City
,BA.STATE
,BA.COUNTRY
,BA.POSTALCODE
,Z.State  ZoneState
,Z.TIMEZONEUTCOFFSET
,(case when Z.TIMEZONEUTCOFFSET is null and BA.State in ('IL', 'MN', 'MO', 'AL', 'TN', 'TX', 'DF', 'JAL', 'NLE', 'QUE', 'MEX','IA','ND','AR','SD','WI','LA','MS','KS','OK') then -6
 when Z.TIMEZONEUTCOFFSET is null and BA.State in ('ON', 'PQ', 'ME', 'FL', 'OH','VA','GA','NJ','NY','SC','QC','MD','MI','CT','IN','PA','MA','NC','KY','DE') then -5
 when Z.TIMEZONEUTCOFFSET is null and BA.State in ('CO','NM','MT','AZ','UT','AB') then -7
 when Z.TIMEZONEUTCOFFSET is null and BA.State in ('SON', 'CA', 'OR','BC','WA','NV') then -8 else Z.TIMEZONEUTCOFFSET end) TimeZone -- there are branches with null citits im modifying STATE if exists if cannot be matched by zip
 

FROM TRUCKLOAD_DOMAIN.DBO.V_DIM_BRANCH as ba -- bringing in Branch Table 
Left Join TRUCKLOAD_DOMAIN.DBO.DIM_GEO_ZIP_CODE as Z on BA.POSTALCODE = Z.ZIPCODE ---- linking zipcode timezones 

Where 
     BA.PrimaryBusinessLineID = 62
)



,LoadProblems as -- Pulling Load Problems table and converting Problem closed Datetime to Central Time
(SELECT
                 LOADPROBLEMS.LoadNum LoadNum
                ,LOADPROBLEMS.EnteredDate EnteredDate
                ,Upper(TRIM(LOADPROBLEMS.EnteredBy)) EnteredBy
                ,LOADPROBLEMS.ClosedDateTime ClosedDateTime
                ,Upper(TRIM(LOADPROBLEMS.ClosedBy)) ClosedBy
                ,1 ActivityCount
                ,Upper(TRIM(LOADPROBLEMS.CODE)) ReasonCode
                , GREATEST
                  (
                      IFNULL(LOAD_CTL.SourceAsOfTS, to_timestamp('1970-01-01 00:00:00')),
                      IFNULL(LOADPROBLEMS.META_LOAD_TIMESTAMP, to_timestamp('1970-01-01 00:00:00'))
                  ) as SourceAsOfTS
                ,LoadProblems.problemseqnum
                ,LoadProblems.code
                  
              , (Case when BTZ.TimeZone is null then LOADPROBLEMS.ClosedDateTime
                      when LOADPROBLEMS.ClosedDateTime is not null and BTZ.TimeZone = -6 then LOADPROBLEMS.ClosedDateTime
                      when LOADPROBLEMS.ClosedDateTime is not null and BTZ.TimeZone = -5 then dateadd(hh, -1, LOADPROBLEMS.ClosedDateTime)
                      when LOADPROBLEMS.ClosedDateTime is not null and BTZ.TimeZone = -7 then dateadd(hh, +1, LOADPROBLEMS.ClosedDateTime)
                      when LOADPROBLEMS.ClosedDateTime is not null and BTZ.TimeZone = -8 then dateadd(hh, +2, LOADPROBLEMS.ClosedDateTime)
                      when LOADPROBLEMS.ClosedDateTime is not null and BTZ.TimeZone = -9 then dateadd(hh, +3, LOADPROBLEMS.ClosedDateTime)
                      when LOADPROBLEMS.ClosedDateTime is not null and BTZ.TimeZone = -4 then dateadd(hh, -2, LOADPROBLEMS.ClosedDateTime)
                      else LOADPROBLEMS.ClosedDateTime end) ClosedDateTimeADJ -- converting closed date time from timezone of the employees' branch to central time.
                  
            FROM
                EXPRESS_RAP.DBO.LOADPROBLEMS
             
                INNER JOIN OPERATIONS_DOMAIN.DBO.STG_SHIPMENT_ACTIVITY_LOADLIST LOAD_CTL
                   ON LOADPROBLEMS.LoadNum = LOAD_CTL.LOADNUM
                 
                left join  EXPRESS_RAP.DBO.EMPLOYEESLOG as EL  on EL.EmpCode = LOADPROBLEMS.ClosedBy and LOADPROBLEMS.ClosedDateTime between EL.StartDate and EL.EndDate
                left join TimeZoneAdjustment as BTZ on BTZ.BRANCHCODE = EL.BRANCHCODE -- matching the employee to the employeelog and branch at the event close time. 
                 
                 
                 --   Where Loadproblems.loadnum in ( '353895523','342524810')-- for testing
       
)

,Events_Updated as  ---- Pulling Events Updated, comparing to Events closed and creating "Remove Flag" column 
(
       SELECT  DISTINCT
            LoadProblemLog.LoadNum LoadNum
            ,'EVENTS UPDATED' Activity
            ,LoadProblemLog.EnteredDateTime ActivityDateTime
            ,LoadProblems.CLoseddatetime 
            ,LoadProblems.CloseddatetimeADJ
            ,1 ActivityCount
            ,Upper(TRIM(LoadProblemLog.EnteredBy)) Employee
            ,Upper(TRIM(LoadProblems.Code)) ReasonCode
            ,NULL IsAutomated
            ,TRUE LoadBasedActivity
  
          --  ,GREATEST
            --  (
              --    IFNULL(LOAD_CTL.SourceAsOfTS, to_timestamp('1970-01-01 00:00:00')),
                --  IFNULL(LoadProblems.META_LOAD_TIMESTAMP, to_timestamp('1970-01-01 00:00:00')),
                  --IFNULL(LoadProblemLog.META_LOAD_TIMESTAMP, to_timestamp('1970-01-01 00:00:00'))
             --) as SourceAsOfTS
              
      
      ------- Comparing events updated entered by to closed by and if closed date time adjusted to Update Dt 
        ,(Case when
                    (case when  trim(upper(LoadProblemLog.enteredby)) = trim(upper(LoadProblems.closedby)) and DATEDIFF(Second,LoadProblems.closedDateTimeADJ,LoadProblemLog.entereddatetime) <30 then 1 else 0 end) = 1 and 
(ROW_NUMBER()
	OVER(PARTITION BY LoadProblems.loadnum, LoadProblems.Code,LoadProblems.closeddatetimeADJ,(case when  trim(upper(LoadProblemLog.EnteredBy)) = trim(upper(LoadProblems.closedby)) and DATEDIFF(SECOND,LoadProblems.closedDateTimeADJ,LoadProblemLog.entereddatetime) <30  then 1 else 0 end)
	ORDER BY LoadProblemLog.entereddatetime desc)) =1 then 1 else 0 end) RemoveFlag  
        
        
        
        FROM
            EXPRESS_RAP.dbo.LoadProblemLog
           -- INNER JOIN EXPRESS_RAP.dbo.LoadProblems
            INNER JOIN LOADPROBLEMS 
                ON LoadProblems.loadnum = LoadProblemLog.loadnum and LoadProblems.problemseqnum = LoadProblemLog.problemseqnum
            INNER JOIN OPERATIONS_DOMAIN.DBO.STG_SHIPMENT_ACTIVITY_LOADLIST LOAD_CTL
                ON LoadProblems.LoadNum = LOAD_CTL.LOADNUM
                
          --Where LoadProblemLog.Loadnum in ( '353895523','342524810')-- for testing lds  2 updates within a min
  
  )
  

  ---- Final  Pulling current Shipment activity table matching Is null values and adding the remove flag. 
  
    SELECT
         a.employee
         ,er.subrollupfamily
         ,ba.branchname
         ,a.moderollup
         ,a.activity 
         ,tc.taskorder 
         ,trim(upper(ba.branchsubregion1)) as BranchRegion
         ,count(distinct a.loadnum) as loads
         ,sum(a.activitycount) as ct
         ,sum(a.activitycycletimeduration)/60 as hrs
         ,Sum(case when EU.REMOVEFLAG =0 then a.activitycycletimeduration/60 else 0 end) as New_Logic_Hrs
         ,sum(EU.REMOVEFLAG) as REMOVEFLAG
FROM Shipment_activity as a
LEFT JOIN  Events_Updated as EU on a.loadnum = EU.Loadnum and a.employee = EU.employee and A.ActivityDatetime = EU.ActivityDatetime and A.Reasoncode = EU.reasoncode

LEFT JOIN TRUCKLOAD_DOMAIN.DBO.V_DIM_BRANCH as ba on ba.branchID = a.employeebranchID 
LEFT JOIN TRUCKLOAD_DOMAIN.DBO.V_DIM_CUSTOMER as c on c.customerid = a.customerid
LEFT JOIN OPERATIONS_DOMAIN.DBO.DIM_TASK_CLASSIFICATION as tc on tc.task = a.activity
LEFT JOIN  OPERATIONS_DOMAIN.DBO.DIM_EMPLOYEE_ROLE as er on trim(upper(a.employeeposition)) = trim(upper(er.psrole))
WHERE 

 ba.primarybusinesslineid = 62
 
GROUP BY        
         a.employee
         ,er.subrollupfamily
         ,ba.branchname
         ,trim(upper(ba.branchsubregion1))
         ,a.moderollup
         ,a.activity 
         ,tc.taskorder   
  
  