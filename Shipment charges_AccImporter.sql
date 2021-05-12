with

SHIPMENT_CHARGES_Accesorial_Importer_addon as -- Running all acc Importer Activity for Q1 date range
(
  select		
a.ENTEREDBYPARTYCODE	
,TIME_SLICE(a.EnteredDate, 30, 'SECOND') as EnteredDate30
,1 as AccUpvol	
  
FROM ORION_RAP.OA.ORDERAUDIT  a

where 
a.categoryid = 7  --8 Load revision --10 =Status Condition Change -- 9 = order revision 7 =financial revision
--and applicationid = 3 ---3 = Orders -- 5 = execution
--and entitytypeid = 1 -- 1= order 2=load 
and a.actionitemtypeid  in (21,22) -- 21= manual rate charge -- 22 =manual rate cost 
and a.sourcesystemid ='479'-- manual entry
and a.fieldname ='Source System'
and a.NewValue = 'Acc Uploader'
and a.entereddate >=  '2021-01-01'
and a.entereddate < '2021-04-01'

----For Testing  
--and a.entereddate >=  '2021-04-07'
--and a.entereddate < '2021-04-08'  
--and a.ENTEREDBYPARTYCODE='PIETAND'
  
Group By
 a.ENTEREDBYPARTYCODE	
  ,TIME_SLICE(a.EnteredDate, 30, 'SECOND')
)


,Shipment_activity as --  Pulling records from shipment activity charges manual activity for q1 

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
,(case when AccUpvol >=1 then 1 else 0 end) as New_Isautomated ---  in case a record exists on the Accessorial importer querry flage it as automated

FROM OPERATIONS_DOMAIN.DBO.SHIPMENT_ACTIVITY a 
LEFT JOIN SHIPMENT_CHARGES_Accesorial_Importer_addon AI on  AI.Entereddate30 = A.ActivityDateTime  -- Join Activity date time
                                                        and AI.ENTEREDBYPARTYCODE = A.EMPLOYEE   -- Join Employee
 WHERE a.ActivityDateTime >= '2021-01-01'
      AND a.ActivityDateTime  < '2021-04-01'
      AND a.ACTIVITY ='SHIPMENT CHARGES'
      AND a.ISAUTOMATED = 'FALSE'  -- pull only manual  records from the shipment activity table for analysis
 
 
 ---- For Testing  -- checking to see if i get the same volume for 1 employee vs operations dashboard. 
--WHERE a.ActivityDateTime >= '2021-04-07'
 --     AND a.ActivityDateTime  < '2021-04-08'
   --   AND a.ACTIVITY ='SHIPMENT CHARGES'
     -- AND a.ISAUTOMATED = 'FALSE'
      --AND a.EMPLOYEE ='PIETAND'
)
                                      
  --- FINAL ---------------- Pull the shipment charges temp table and aggregate it for region branch etc.                                               
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
         ,Sum(case when A.New_Isautomated =0 then a.activitycycletimeduration/60 else 0 end) as New_Logic_Hrs
         ,sum(a.New_isautomated) as New_Isautomated
FROM Shipment_activity as a

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
                                                       
                                                       
                                                       
                                                       
                                                       
                                                       
                                                       
                                                       
                                                       
 