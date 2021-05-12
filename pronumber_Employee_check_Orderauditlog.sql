with 

Shipment_activity as --  Pulling pronumber manual activity for q1 

(Select 
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
 
FROM OPERATIONS_DOMAIN.DBO.SHIPMENT_ACTIVITY a

WHERE a.ActivityDateTime >= '2021-01-01'
      AND a.ActivityDateTime  < '2021-04-01'
      AND a.ACTIVITY ='PRONUM ENTRIES'
      AND a.ISAUTOMATED = 'FALSE'
 )   
      
,New_Pronum_logic as  --Revising Jeejesh's ops domain logic and linking it to shipment activiy

(SELECT DISTINCT 
            TCA.KeyColumnValue LoadNum
            ,'PRONUM ENTRIES' Activity1
            ,TCA.AuditDatetime ActivityDateTime
            ,1 ActivityCount
            ,Upper(Trim(TCA.AuditUserId)) Employee
            ,Upper(Trim(TCA.AuditType)) ReasonCode
            ,(case  when TCA.AUDITCOLUMNNEWVALUE is null then 0 -- at times users remove the order number leaving the filed blank so if new value is null its manual activity. 
                    when Upper(Trim(OA.ENTEREDBYPARTYCODE)) is null then 1 else 0 end) as IsAutomatedA -- when order audit as a manual entry is not found then automate
            ,TRUE LoadBasedActivity
            --,GREATEST
             --  (
              --    IFNULL(LOAD_CTL.SourceAsOfTS, to_timestamp('1970-01-01 00:00:00')),
              --    IFNULL(TCA.HVR_CAPTURE_DATE, to_timestamp('1970-01-01 00:00:00'))
              -- ) as SourceAsOfTS
               
            -- ,TCA.AuditColumnNewValue  -- for testing
           -- ,OA.ENTEREDBYPARTYCODE  -- for tesitng
           
        FROM
            OPERATIONS_DOMAIN.DBO.V_HVR_DBO_TABLECOLUMNAUDIT TCA
            INNER JOIN Shipment_activity SA ON TCA.KeyColumnValue = SA.Loadnum-- Puling load numbers from shipment activity with manual pronum activity

            LEFT  JOIN  ORION_RAP.OA.ORDERAUDIT OA on  OA.entityID = TCA.KeyColumnValue -- matching load num
                                                   AND OA.NewValue = TCA.AuditColumnNewValue -- maching actual pronum as a new value
                                                   AND Upper(Trim(OA.ENTEREDBYPARTYCODE))=Upper(Trim(TCA.AuditUserId)) --matching entred by
                                                   AND OA.EntityTypeID = 2 -- 2 = loadnum 1 = ordernum
                                                   AND OA.ACTIONITEMTYPEID  in (5,31) --- 5 = addrefnum -- 31 Update refnum
                                                   AND OA.FIELDNAME = 'Value'
                                                   AND OA.SOURCESYSTEMID in (479,3956) -- 479 =manual entry and 3956 = execution(done in load)
                                                   AND OA.FIELDPARENT like ('T%')
                                                   
                        
        WHERE 
         -- TCA.KeyColumnValue  in ('333110799','339911444','353987736',351544078,354023379,'354003389','354003092')  -- for testing
             TCA.TableName = 'LOADBOOKS'
            AND TCA.AuditColumnName = 'CARRIERPRONUMBER'
            AND TCA.AuditDateTime >= '2021-01-01'
           AND TCA.AuditDateTime < '2021-04-01'
)


--FINAL OUTPUT
     
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
         ,Sum(case when NP.IsautomatedA =0 then a.activitycycletimeduration/60 else 0 end) as New_Logic_Hrs
         ,sum(NP.IsautomatedA) as New_Isautomated
FROM Shipment_activity as a
LEFT JOIN  New_Pronum_logic as NP  on a.loadnum = NP.Loadnum and a.employee = NP.employee and A.ActivityDatetime = NP.ActivityDatetime

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
   
 

  
  
  

 
  