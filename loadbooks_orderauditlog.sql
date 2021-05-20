
        with lb_base
            (Loadnum, Activity, ActivityDateTime, ActivitySmallDateTime, ActivityCount,Employee,IsAutomated,LoadBasedActivity,SourceAsOfTS) AS
        (
            SELECT
                 oa.entityID Loadnum
                ,'LOADS BOOKED' Activity
                ,oa.entereddate ActivityDateTime
                ,date_trunc('MINUTE',oa.entereddate) as ActivitySmallDateTime
                ,1 ActivityCount
                ,oa.enteredbypartycode Employee
                ,CASE
                    WHEN oa.sourcesystemid in (480,481,2209,2210,3547,4194,5140,5326,5391) then 1
                    ELSE NULL 
                 END isAutomated
                ,1 as LoadBasedActivity ----do we call this load based activity if it's originating from order audit log? but its booking at the load level?
                ,oa.entereddate SourceAsOfTS
            FROM ORION_RAP.OA.ORDERAUDIT oa
            INNER JOIN OPERATIONS_DOMAIN.DBO.STG_SHIPMENT_ACTIVITY_LOADLIST ORDER_CTL
                ON ORDER_CTL.LOADNUM = oa.entityID
            WHERE 
                oa.categoryid = 10
                AND oa.entitytypeid = 2
                AND oa.applicationid = 5   
                AND oa.actiontypeid = 9
                AND oa.actionitemtypeid = 42
                AND cast(oa.entereddate as date) >= '2021-03-01'
                AND cast(oa.entereddate as date) < '2021-04-01'
                AND trim(upper(oa.newvalue)) ='BOOKED'
          
            GROUP BY
                oa.entityID 
                ,'LOADS BOOKED'
                ,oa.entereddate 
                ,date_trunc('MINUTE',oa.entereddate) 
                ,1 
                ,oa.enteredbypartycode
                ,CASE
                    WHEN oa.sourcesystemid in (480,481,2209,2210,3547,4194,5140,5326,5391) then 1
                    ELSE NULL 
                 END 
                 ,ORDER_CTL.LOADNUM
            QUALIFY row_number() over (PARTITION BY LoadNum,Employee,ActivitySmallDateTime 
                                    ORDER BY LoadNum,Employee,ActivitySmallDateTime ASC) = 1
)
,lb_base2 

            (Loadnum,OrderNum, Activity, ActivityDateTime, ActivitySmallDateTime, ActivityCount,Employee,IsAutomated,LoadBasedActivity,SourceAsOfTS) AS
        (
                   SELECT
                 ORDER_CTL.loadnum
                ,oa.entityID OrderNum
                ,'LOADS BOOKED' Activity
                ,oa.entereddate ActivityDateTime
                ,date_trunc('MINUTE',oa.entereddate) as ActivitySmallDateTime
                ,1 ActivityCount
                ,oa.enteredbypartycode Employee
                ,CASE
                    WHEN oa.sourcesystemid in (480,481,2209,2210,3547,4194,5140,5326,5391) then 1
                    ELSE NULL 
                 END isAutomated
                ,1 as LoadBasedActivity ----do we call this load based activity if it's originating from order audit log? but its booking at the load level?
                ,oa.entereddate SourceAsOfTS
        
            FROM ORION_RAP.OA.ORDERAUDIT oa
            INNER JOIN OPERATIONS_DOMAIN.DBO.STG_SHIPMENT_ACTIVITY_ORDERLIST ORDER_CTL
                ON ORDER_CTL.ORDERNUM = oa.entityID
            WHERE 
                oa.categoryid = 10
                AND oa.entitytypeid = 1
                AND oa.applicationid = 3   
                AND oa.actiontypeid = 16
               AND oa.actionitemtypeid = 42
                AND cast(oa.entereddate as date) >= '2021-03-01'
                AND cast(oa.entereddate as date) < '2021-04-01' 
                AND trim(upper(oa.newvalue)) ='BOOKED'
          
            GROUP BY
                ORDER_CTL.loadnum
                ,oa.entityID 
                ,'LOADS BOOKED'
                ,oa.entereddate 
                ,date_trunc('MINUTE',oa.entereddate) 
                ,1 
                ,oa.enteredbypartycode
                ,CASE
                    WHEN oa.sourcesystemid in (480,481,2209,2210,3547,4194,5140,5326,5391) then 1
                    ELSE NULL 
                 END
          ,ORDER_CTL.ORDERNUM
      
                   QUALIFY row_number() over (PARTITION BY OrderNum,LoadNum,Employee,ActivitySmallDateTime 
                                    ORDER BY OrderNum,LoadNum,Employee,ActivitySmallDateTime ASC) = 1
          )


        ,TEMP_LOAD_ORDER_ACTIVITY
            (Loadnum, Activity, ActivityDateTime, ActivityCount,Employee,IsAutomated,LoadBasedActivity,SourceAsOfTS)  AS          
        (
          SELECT
                 Loadnum
                ,Activity
                ,ActivityDateTime
                ,ActivityCount
                ,Employee
                ,isAutomated
                ,loadbasedactivity
                ,sourceasofts
          FROM lb_base
          GROUP BY                 
                 Loadnum
                ,Activity
                ,ActivityDateTime
                ,ActivityCount
                ,Employee
                ,isAutomated
                ,loadbasedactivity
                ,sourceasofts 
          
          UNION
           SELECT
                 Loadnum
                ,Activity
                ,ActivityDateTime
                ,ActivityCount
                ,Employee
                ,isAutomated
                ,loadbasedactivity
                ,sourceasofts
          FROM lb_base2
          GROUP BY                 
                 Loadnum
                ,Activity
                ,ActivityDateTime
                ,ActivityCount
                ,Employee
                ,isAutomated
                ,loadbasedactivity
                ,sourceasofts 

       )
       select * from TEMP_LOAD_ORDER_ACTIVITY where ISAUTOMATED IS NULL


--2.8M
--1.792M



with LOAD_BOOKED_BOUNCED_DRIVERINFO as
        (
            SELECT
                LoadBooks.LoadNum LoadNum
                ,LoadBooks.BookedDate BookedDate
                ,Upper(TRIM(LoadBooks.BookedBy)) BookedBy
                ,LoadBooks.Bounced Bounced
                ,LoadBooks.BouncedDate BouncedDate
                ,Upper(TRIM(LoadBooks.BouncedBy)) BouncedBy
                ,LoadBooks.bouncedcode BouncedCode
                ,LoadBooks.DriverInfoDate DriverInfoDate
                ,Upper(TRIM(LoadBooks.DriverInfoBy)) DriverInfoBy
                ,1 ActivityCount
                , GREATEST
                (
                    IFNULL(LoadBooks.updateddate, to_timestamp('1970-01-01 00:00:00')),
                    IFNULL(LoadBooks.entereddate, to_timestamp('1970-01-01 00:00:00')),
                    IFNULL(LOAD_CTL.SourceAsOfTS, to_timestamp('1970-01-01 00:00:00'))
                ) as SourceAsOfTS               
            FROM
                EXPRESS_RAP.dbo.LoadBooks
                INNER JOIN OPERATIONS_DOMAIN.DBO.STG_SHIPMENT_ACTIVITY_LOADLIST LOAD_CTL
                    ON LoadBooks.LoadNum = LOAD_CTL.LOADNUM
            WHERE
                (LoadBooks.BookedDate IS NOT NULL OR
                    (LoadBooks.Bounced  AND LoadBooks.BouncedDate  IS NOT NULL ) OR
                     LoadBooks.DriverInfoDate IS NOT NULL)
        )
       ,prod_lb as
       (
        SELECT DISTINCT
            LOAD_BOOKED_BOUNCED_DRIVERINFO.LoadNum ,
            'LOADS BOOKED' Activity,
            LOAD_BOOKED_BOUNCED_DRIVERINFO.BookedDate ActivityDateTime,
            LOAD_BOOKED_BOUNCED_DRIVERINFO.ActivityCount ,
            CASE WHEN LOAD_BOOKED_BOUNCED_DRIVERINFO.BookedBy = 'VERIFY' then Upper(TRIM(Loadmatch.CarrierRep)) else LOAD_BOOKED_BOUNCED_DRIVERINFO.BookedBy end BookedBy ,
            NULL ReasonCode ,
            NULL IsAutomated ,
            TRUE LoadBasedActivity ,
            LOAD_BOOKED_BOUNCED_DRIVERINFO.SourceAsOfTS
        FROM
            LOAD_BOOKED_BOUNCED_DRIVERINFO
            INNER JOIN EXPRESS_RAP.dbo.Loadmatch ON
                Loadmatch.LoadNum = LOAD_BOOKED_BOUNCED_DRIVERINFO.LoadNum
        WHERE

        LOAD_BOOKED_BOUNCED_DRIVERINFO.BookedDate >= '2021-03-01'
         and LOAD_BOOKED_BOUNCED_DRIVERINFO.BookedDate < '2021-04-01'
 )                  
                     --110970330 - 30 sec
                     --38.5k - 6 min
   select * FROM prod_lb
