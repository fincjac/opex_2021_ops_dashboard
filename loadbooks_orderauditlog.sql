SELECT 
 oa.entityid --ordernum
,A.Loadnumber --Loadnum
,oa.entereddate --Date_Entered
,oa.enteredbypartycode --Employee
,'Loads Booked' --Task
,1 --Volume
,hour(cast(oa.entereddate as timestamp)) --Hour_entered
,LP2.PartyCode --C_Code
,SO.Description --Load_Type
,oa.sourcesystem --Order_Source
,CASE WHEN oa.sourcesystemid in (481,480,5326,5258,2209,4226,5391,2210,4968,5140,4791,5191,5468,4757) then 1 ELSE null END --Automated
,null --Reason_Code
,LP1.PartyCode --customerbranch	
,null --DL_RNB
,case when bot.automated = 'Auto' then 1 else null end --bot_flag
--,el.branchcode -- as emp_branch
--,el.psrole --as emp_psrole

FROM ORION_RAP.OA.ORDERAUDIT as oa

	inner join ORION_RAP.co.order_ as o on oa.entityid = o.CustomerOrderNumber
	inner join  OPERATIONS_DOMAIN.DBO.OPEX_BOTS as bot on bot.empcode = oa.enteredbypartycode

	inner join  ORION_RAP.EP.Activity as A on o.CustomerOrderNumber = A.ordernumber

	inner join ORION_RAP.co.Service as S on S.CustomerOrderID = O.CustomerOrderID
         
    inner join ORION_RAP.ref.serviceoffering as SO on SO.ServiceOfferingID = S.CHRWServiceOfferingNumber

    inner join ORION_RAP.co.OrderParty as OP1  on O.CustomerOrderID = OP1.CustomerOrderID --Owning Office

    inner join MDM_RAP.mdm.LightParty as LP1  on OP1.PartyNumber = LP1.PartyNumber --Owning Office
       
    inner join ORION_RAP.co.OrderParty as OP2  on O.CustomerOrderID = OP2.CustomerOrderID  --Customer

    inner join MDM_RAP.mdm.LightParty as LP2  on OP2.PartyNumber = LP2.partynumber  --Customer
   
    LEFT JOIN CDC_EXPRESS.HVR.DBO_EMPLOYEES E ON E.EmpCode = oa.enteredbypartycode  --Enteredby
       
    inner join MDM_RAP.mdm.BranchAccounting_vw as BA  on BA.BranchNumber = LP1.PartyCode --customerbranch
    
   -- INNER JOIN edl.report_common_dbo_employeeslog as el on upper(trim(el.empcode)) = upper(trim(oa.enteredbypartycode))

WHERE 
oa.categoryid = 10  --8 Load revision --10 =Status Condition Change
and oa.applicationid = 5 ---3 = Orders -- 5 = execution
and oa.entitytypeid = 2 -- 1= order 2=load 
and oa.actiontypeid = 9 -- 7 =Load Create 10 =load Update  16 =Order Status Change  9 =Load Status change
and oa.actionitemtypeid = 42 -- 6=  add stop 34 = update stop Information Updated 42 = Information Updated
--and oa.sourcesystemid ='479'-- manual entry
and  oa.fieldname ='Booked'
and oa.entereddate >= '2021-02-01' --cast(date_sub(current_date,) as date)
and oa.entereddate < '2021-02-03'-- cast(date_sub(current_date,0) as date)
and OP1.PartyRoleRDN = '361' --OwningOffice
and OP2.PartyRoleRDN = '7' --Customer
and ba.primarybusinesslineid = 62
---and oa.entereddate >= el.startdate and oa.entereddate <= el.enddate
--and oa.entityid = '129094341' -- for testing

GROUP BY 
oa.entityid --ordernum
,A.Loadnumber --Loadnum
,oa.entereddate --Date_Entered
,oa.enteredbypartycode --Employee
,'Loads Booked' --Task
,hour(cast(oa.entereddate as timestamp)) --Hour_entered
,LP2.PartyCode --C_Code
,SO.Description --Load_Type
,oa.sourcesystem --Order_Source
,CASE WHEN oa.sourcesystemid in (481,480,5326,5258,2209,4226,5391,2210,4968,5140,4791,5191,5468,4757) then 1 ELSE null END --Automated
,null --Reason_Code
,LP1.PartyCode --customerbranch	
,null --DL_RNB
,case when bot.automated = 'Auto' then 1 else null end --bot_flag
--, el.branchcode -- as emp_branch
---, el.psrole --as emp_psrole
