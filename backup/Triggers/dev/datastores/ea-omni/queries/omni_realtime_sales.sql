

WITH MatrixCell
AS (
 SELECT MatrixCellId
  ,Code
  ,Name
  ,MatrixCellAk
  ,AttributeSetId
  ,MatrixSheetId
 FROM CNF_MatrixCell WITH (NOLOCK)
 )
 ,atsd
AS (
 SELECT  AttributeSetId
  ,AttributeId
 FROM CNF_AttributeSetDetail WITH (NOLOCK)
 )
 ,att
AS (
 SELECT AttributeId
  ,AttributeTemplateId
  ,Name
  ,Code
 FROM CNF_Attribute WITH (NOLOCK)
 )
 ,sgv
AS (
 SELECT MatrixCellId
  ,StatisticalGroupId
 FROM CNF_StatisticalGroupValidity WITH (NOLOCK)
 )
 ,sg
AS (
 SELECT StatisticalGroupId
  ,StatisticalGroupCode
  ,StatisticalGroupDescription
  ,ParentId
  ,Position
  ,LEVEL
 FROM CNF_StatisticalGroup WITH (NOLOCK)
 )
 ,LookUp
AS (
 SELECT *
 FROM [dbo].[CNF_LookUp] WITH (NOLOCK)
 )
 ,Performance
AS (
 SELECT *
 FROM CNF_Performance WITH (NOLOCK)
 )
 ,Event
AS (
 SELECT *
 FROM CNF_Event WITH (NOLOCK)
 )
 ,Location
AS (
 SELECT *
 FROM CNF_Location WITH (NOLOCK)
 )
 ,EA_User
AS (
 SELECT *
 FROM [dbo].[CNF_User] WITH (NOLOCK)
 )
 ,MatrixSheet
AS (
 SELECT *
 FROM [dbo].[CNF_MatrixSheet] WITH (NOLOCK)
 )
 ,EA_Capacity
AS (
 select PerformanceId
 ,SeatingCategoryCode,EVENTID ,EVENTCODE ,Performancename,SeatingCategoryCapacity
 ,sum(SeatingCategoryCapacity) over (partition by  PERFORMANCEID) as ArenaCapacity
 from (
 select PerformanceId
 ,SeatingCategoryCode,EVENTID ,EVENTCODE ,Performancename 
 ,sum(ArenaCapacity) as SeatingCategoryCapacity
 from (
 select 
 cp.PerformanceId
 ,csc.code as SeatingCategoryCode
 ,cec.Quantity as ArenaCapacity
 ,ce.EVENTID ,ce.EVENTCODE ,cp.Performancename 
 from
 CNF_Performance cp
 join cnf_event ce on cp.EventId=ce.EventId
 join CNF_SeatCategory csc on csc.PerformanceId=cp.PerformanceId
 join CNF_EnvelopeCapacity cec on cec.SeatCategoryId=csc.SeatCategoryId
 join CNF_Location Loc on Loc.LocationId=ce.LocationId
 join CNF_SpaceStructure sst on cec.SpaceStructureId= sst.SpaceStructureId and cp.PerformanceId=sst.PerformanceId
 join CNF_Envelope env on cec.EnvelopeId=env.EnvelopeId
 where env.Code not in ('SD','KILL','BLOCK')
 ) x group by PerformanceId,SeatingCategoryCode,EVENTID ,EVENTCODE ,Performancename 
 ) y group by PerformanceId ,SeatingCategoryCode,EVENTID ,EVENTCODE ,Performancename ,SeatingCategoryCapacity
 ) 
 ,
 -- Exploding Attributes
att_exploded
AS (
 SELECT atsd.AttributeSetId
  ,MAX(CASE 
    WHEN att.AttributeTemplateId = 1
     THEN att.Name
    END) AS TicketType_Name
  ,MAX(CASE 
    WHEN att.AttributeTemplateId = 1
     THEN att.Code
    END) AS TicketType_Code
  ,MAX(CASE 
    WHEN att.AttributeTemplateId = 3
     THEN att.Name
    END) AS Classification_Name
  ,MAX(CASE 
    WHEN att.AttributeTemplateId = 3
     THEN att.Code
    END) AS Classification_Code
  ,MAX(CASE 
    WHEN att.AttributeTemplateId = 5
     THEN att.Name
    END) AS Events_Name
  ,MAX(CASE 
    WHEN att.AttributeTemplateId = 5
     THEN att.Code
    END) AS Events_Code
  ,MAX(CASE 
    WHEN att.AttributeTemplateId = 6
     THEN att.Name
    END) AS Sections_Name
  ,MAX(CASE 
    WHEN att.AttributeTemplateId = 6
     THEN att.Code
    END) AS Sections_Code
  ,MAX(CASE 
    WHEN att.AttributeTemplateId = 11
     THEN att.Name
    END) AS SeatType_Name
  ,MAX(CASE 
    WHEN att.AttributeTemplateId = 11
     THEN att.Code
    END) AS SeatType_Code
 FROM atsd
 JOIN att ON atsd.AttributeId = att.AttributeId
 GROUP BY atsd.AttributeSetId
 ) 
 ,
 -- Statistical Group Aggregation
sg_agg
AS (
 SELECT StatisticalGroupId
  ,StatisticalGroupCode
  ,StatisticalGroupDescription
  ,ParentId
  ,Position
  ,LEVEL
 FROM sg
 )
 ,
 -- Mapping Statistical Groups
sgm
AS (
 SELECT a.StatisticalGroupId AS L1_Id
  ,a.StatisticalGroupCode AS L1_Code
  ,a.StatisticalGroupDescription AS L1_Description
  ,a.LEVEL AS L1
  ,b.StatisticalGroupId AS L2_Id
  ,b.StatisticalGroupCode AS L2_Code
  ,b.StatisticalGroupDescription AS L2_Description
  ,b.LEVEL AS L2
  ,c.StatisticalGroupId AS L3_Id
  ,c.StatisticalGroupCode AS L3_Code
  ,c.StatisticalGroupDescription AS L3_Description
  ,c.LEVEL AS L3
 FROM sg_agg a
 LEFT JOIN sg_agg b ON a.StatisticalGroupId = b.ParentId
 LEFT JOIN sg_agg c ON b.StatisticalGroupId = c.ParentId
 WHERE a.ParentId IS NULL
 )
 ,sgmap
AS (
 SELECT DISTINCT L3_ID AS ID
  ,L1_ID
  ,L1_CODE
  ,L1_DESCRIPTION
  ,L1
  ,L2_ID
  ,L2_CODE
  ,L2_DESCRIPTION
  ,L2
  ,L3_ID
  ,L3_CODE
  ,L3_DESCRIPTION
  ,L3
 FROM sgm
 
 UNION
 
 SELECT DISTINCT L2_ID AS ID
  ,L1_ID
  ,L1_CODE
  ,L1_DESCRIPTION
  ,L1
  ,L2_ID
  ,L2_CODE
  ,L2_DESCRIPTION
  ,L2
  ,NULL
  ,NULL
  ,NULL
  ,NULL
 FROM sgm
 
 UNION
 
 SELECT DISTINCT L1_ID AS ID
  ,L1_ID
  ,L1_CODE
  ,L1_DESCRIPTION
  ,L1
  ,NULL
  ,NULL
  ,NULL
  ,NULL
  ,NULL
  ,NULL
  ,NULL
  ,NULL
 FROM sgm
 )
 ,
 -- Mapped Statistical Group
sg_mapped
AS (
 SELECT DISTINCT sgv.MatrixCellId
  ,sg.STATISTICALGROUPID AS sg_id
  ,STATISTICALGROUPCODE AS sg_code
  ,STATISTICALGROUPDESCRIPTION AS sg_description
  ,PARENTID AS sg_parent_id
  ,sg.POSITION AS sg_position
  ,sg.LEVEL AS sg_level
  ,L1_ID
  ,L1_CODE
  ,L1_DESCRIPTION
  ,L1
  ,L2_ID
  ,L2_CODE
  ,L2_DESCRIPTION
  ,L2
  ,L3_ID
  ,L3_CODE
  ,L3_DESCRIPTION
  ,L3
 FROM sgv
 JOIN sg ON sgv.StatisticalGroupId = sg.StatisticalGroupId
 JOIN sgmap ON sg.StatisticalGroupId = sgmap.Id
 ) 
 ,DWH_DIM_PRODUCT
AS (
 SELECT 'Omni - Viva' AS SOURCE_SYSTEM_NAME
  ,'EA' AS FACILITY_CODE
  ,mc.Code AS PRODUCT_CODE
  ,mc.Name AS PRODUCT_NAME
  ,mc.MatrixCellAk AS PRODUCT_AK
  ,att.Events_Name AS EVENT_NAME
  ,att.Events_Code AS EVENT_CODE
  ,att.Sections_Code AS SEATING_CATEGORY_CODE
  ,COALESCE(att.TicketType_Name, 'Ticket') AS PRODUCT_TYPE_NAME
  ,att.TicketType_Code AS PRODUCT_TYPE_CODE
  ,att.Classification_Code AS ADULT_CHILD_CODE
  ,att.Classification_Name AS ADULT_CHILD_NAME
  ,mc.Code AS TICKET_CODE
  ,mc.Name AS TICKET_NAME
  ,att.Sections_Code AS TICKET_TIER_CODE
  ,att.Sections_Name AS TICKET_TIER_NAME
  ,CASE 
   WHEN sg_code IS NOT NULL
    THEN CONCAT (
      sg_code
      ,':'
      ,sg_description
      )
   ELSE NULL
   END AS ATTRIBUTE_1
  ,CASE 
   WHEN L1_CODE IS NOT NULL
    THEN CONCAT (
      L1_CODE
      ,':'
      ,L1_DESCRIPTION
      )
   ELSE NULL
   END AS ATTRIBUTE_2
  ,CASE 
   WHEN L2_CODE IS NOT NULL
    THEN CONCAT (
      L2_CODE
      ,':'
      ,L2_DESCRIPTION
      )
   ELSE NULL
   END AS ATTRIBUTE_3
  ,CASE 
   WHEN L3_CODE IS NOT NULL
    THEN CONCAT (
      L3_CODE
      ,':'
      ,L3_DESCRIPTION
      )
   ELSE NULL
   END AS ATTRIBUTE_4
  ,'Y' AS CURRENT_FLAG
  ,GETDATE() AS CREATED_DATETIME
  ,GETDATE() AS MODIFIED_DATETIME
 FROM MatrixCell mc
 LEFT JOIN att_exploded att ON mc.AttributeSetId = att.AttributeSetId
 LEFT JOIN sg_mapped ON mc.MatrixCellId = sg_mapped.MatrixCellId
 ) 
 ,dd
AS (
 SELECT PRODUCT_AK AS pkey
  ,MAX(CAST(CREATED_DATETIME AS DATETIME)) AS udate
 FROM DWH_DIM_PRODUCT AS DIM_VIVA_PRODUCT
 WHERE SOURCE_SYSTEM_NAME = 'Omni - Viva'
  AND CURRENT_FLAG = 'Y'
  AND (
   Attribute_3 NOT LIKE 'GROUPTYPE%'
   OR Attribute_3 IS NULL
   )
 GROUP BY PRODUCT_AK
 )
 ,ddg
AS (
 SELECT PRODUCT_AK AS pkey
  ,MAX(CAST(CREATED_DATETIME AS DATETIME)) AS udate
 FROM DWH_DIM_PRODUCT AS DIM_VIVA_PRODUCT
 WHERE SOURCE_SYSTEM_NAME = 'Omni - Viva'
  AND CURRENT_FLAG = 'Y'
  AND Attribute_3 LIKE 'GROUPTYPE%'
 GROUP BY PRODUCT_AK
 )
 ,grp
AS (
 SELECT PRODUCT_AK
  ,MAX(CASE 
    WHEN ATTRIBUTE_1 LIKE 'G%:%'
     THEN CAST(SUBSTRING(ATTRIBUTE_1, CHARINDEX('G', ATTRIBUTE_1) + 1, CHARINDEX(':', ATTRIBUTE_1) - CHARINDEX('G', ATTRIBUTE_1) - 1) AS INT)
    END) quantity
 FROM ddg
 JOIN DWH_DIM_PRODUCT ON ddg.pkey = DWH_DIM_PRODUCT.PRODUCT_AK
 WHERE CREATED_DATETIME = udate
  AND SOURCE_SYSTEM_NAME = 'Omni - Viva'
  AND CURRENT_FLAG = 'Y'
  AND Attribute_3 LIKE 'GROUPTYPE%'
 GROUP BY PRODUCT_AK
 )
 ,DIM_VIVA_PRODUCT
AS (
 SELECT *
  ,CASE 
   WHEN quantity_multiplier != 1
    THEN 'TRUE'
   ELSE 'FALSE'
   END AS "Viva Product Quantity Multiplier"
 FROM (
  SELECT prd.PRODUCT_CODE
   ,prd.PRODUCT_NAME
   ,prd.PRODUCT_AK
   ,prd.EVENT_CODE
   ,prd.EVENT_NAME
   ,prd.TICKET_CODE
   ,prd.TICKET_NAME AS "Viva Product Ticket Name"
   ,prd.SEATING_CATEGORY_CODE
   ,prd.ATTRIBUTE_1 AS "Viva Product Statistical Group"
   ,prd.ATTRIBUTE_2 AS "Viva Product Statistical Group Level 1"
   ,prd.ATTRIBUTE_3 AS "Viva Product Statistical Group Level 2"
   ,prd.ATTRIBUTE_4 AS "Viva Product Statistical Group Level 3"
   ,COALESCE(grp.quantity, CASE 
     WHEN CHARINDEX('Group of ', prd.PRODUCT_NAME) > 0
      THEN SUBSTRING(prd.PRODUCT_NAME, CHARINDEX('Group of ', prd.PRODUCT_NAME) + 9, PATINDEX('%[0-9]%', SUBSTRING(prd.PRODUCT_NAME, CHARINDEX('Group of ', prd.PRODUCT_NAME) + 9, LEN(prd.PRODUCT_NAME) - CHARINDEX('Group of ', prd.PRODUCT_NAME))))
     END, 1) AS quantity_multiplier
  FROM dd
  JOIN DWH_DIM_PRODUCT prd ON prd.PRODUCT_AK = pkey
   AND prd.CREATED_DATETIME = udate
   --AND prd.SOURCE_SYSTEM_NAME = 'Omni - Viva'
   AND prd.CURRENT_FLAG = 'Y'
   AND (
    prd.Attribute_3 NOT LIKE 'GROUPTYPE%'
    OR prd.Attribute_3 IS NULL
    )
  LEFT JOIN grp ON grp.PRODUCT_AK = pkey
  WHERE NOT (prd.ATTRIBUTE_1 LIKE '%Dynamic Q%' 
    OR prd.ATTRIBUTE_2 LIKE '%Dynamic Q%' 
    OR prd.ATTRIBUTE_3 LIKE '%Dynamic Q%' 
    OR prd.ATTRIBUTE_4 LIKE '%Dynamic Q%'
)
  ) a
 )
 ,DIM_PRODUCT
AS (
 SELECT DISTINCT PRODUCT_CODE
  ,PRODUCT_NAME
  ,PRODUCT_AK
  ,EVENT_NAME
  ,EVENT_CODE
  ,SEATING_CATEGORY_CODE
  ,"Viva Product Statistical Group"
  ,"Viva Product Statistical Group Level 1"
  ,"Viva Product Statistical Group Level 2"
  ,"Viva Product Statistical Group Level 3"
  ,"Viva Product Ticket Name"
  ,"Viva Product Quantity Multiplier"
  ,quantity_multiplier
 FROM DIM_VIVA_PRODUCT
 ) 
 ,SaleItemNote
AS (
 SELECT dn.NoteDate
  ,dn.Note
  ,cun.UserCode AS SupervisorID
  ,cun.UserName AS Supervisor
  ,ds.SaleId
  ,cus.UserCode AS CashierID
  ,cus.UserName AS Cashier
 FROM [dbo].[DATA_Note] dn
 INNER JOIN [dbo].[DATA_Sale] ds ON ds.SaleId = dn.NoteKey
 INNER JOIN EA_User cus ON cus.UserId = ds.UserId
 INNER JOIN EA_User cun ON cun.UserId = dn.UserId
 WHERE dn.Note IS NOT NULL
  AND upper(dn.note) LIKE '%SUPERVISOR OVERRIDE%'
 )
 ,SaleItemPromotion
AS (
 SELECT SA.SaleId
  ,SI.SaleItemId
  ,MC.MatrixCellId
  ,MC.Code
  ,MC.MatrixCellAk AS PRODUCT_AK
  ,MS.ProductType
  ,(SI.UnitRawPrice * SI.Quantity) AS GrossAmount
  ,SI.TotalAmount AS NetAmount
  ,P.PromotionId
  ,P.Code PromotionCode
  ,P.Description PromotionDescription
 FROM [dbo].[DATA_Sale] SA WITH (NOLOCK)
 INNER JOIN [dbo].[DATA_SaleItem] SI WITH (NOLOCK) ON SI.SaleId = SA.SaleId
 INNER JOIN MatrixCell MC ON MC.MatrixCellId = SI.MatrixCellId
 INNER JOIN MatrixSheet MS ON MS.MatrixSheetId = MC.MatrixSheetId
 INNER JOIN [dbo].[DATA_SaleItemRule] SIR WITH (NOLOCK) ON SIR.SaleItemId = SI.SaleItemId
 INNER JOIN [dbo].[CNF_PromotionItemRule] PIR WITH (NOLOCK) ON PIR.PromotionItemRuleId = SIR.PromotionItemRuleId
 INNER JOIN [dbo].[CNF_PromotionItem] PI WITH (NOLOCK) ON PI.promotionitemid = PIR.PromotionItemId
 INNER JOIN [dbo].[CNF_Promotion] P WITH (NOLOCK) ON P.PromotionId = PI.PromotionId
 INNER JOIN LookUp LK ON LK.ItemId = P.PromotionType
  AND LK.LookUpTableId = 217
 WHERE
  -- SA.Completed = 1
  SI.Quantity <> 0
 )  
 ,INTER_SALES_FACT_NEW
AS (
 SELECT loc.LocationCode AS Facility_Code
  ,loc.Name AS Facility_Name
  ,ce.EventCode
  ,ce.eventid AS EVENT_ID
  ,ce.EventAK
  ,lkp.Description Event_Type
  ,ce.EventName
  ,LkpS.Description Event_Status
  ,sa.cp_PerformanceName AS PerformanceName
  ,ce.StartDate AS EVENT_STARTDATE
  ,ce.EndDate AS EVENT_ENDDATE
  ,SA.SaleId
  ,SA.SaleDate
  ,SA.SaleType
  ,TCK.TicketId
  ,MC.Code
  ,MC.Name
  ,MC.MatrixCellAk
  ,MC.Code AS PRODUCT_CODE
  ,MC.Name AS PRODUCT_NAME
  ,MC.MatrixCellAk AS PRODUCT_AK
  ,MS.ProductType
  ,TCK.VoidType
  ,TCK.ActiveFrom
  ,TCK.ActiveTo
  ,TCK.ActiveTo AS Expiry_Date
  ,TCK.PrintDateTime
  ,LastMod.TransactionDate LastModifiedDate
  ,LastMod.TransactionType
  ,SA.s2p_PerformanceId AS PerformanceId
  ,SA.cp_PerfDateTime AS PerfDateTime
  ,Promo.PromotionCode
  ,Promo.PromotionDescription
  ,SA.si_quantity AS quantity
  ,SI_UnitPrice AS UnitPrice
  ,(si_UnitRawPrice - SI_UnitPrice) AS UnitDiscount
  ,(si_UnitPrice * si_Quantity) AS TotalAmount
  ,(si_UnitRawPrice - si_UnitPrice) * si_Quantity AS TotalDiscount
  ,R.ReservationAK
  ,VOID.Description AS VOID_TYPE_DESC
  ,SA.[STATUS]
  ,StatusCodeExt --TRANSACTION_STATUS_DESC      
  ,U.UserCode
  ,U.UserName
  ,U.RoleNames
  ,7 AS Source_System_Code
  ,TicketCatalogue.TicketCode
 FROM (
  SELECT SA.SaleId
  ,SA.SaleDate
  ,SA.SaleType
  ,(
   CASE SA.Approved
    WHEN 0
     THEN '_'
    ELSE 'A'
    END
   ) + (
   CASE SA.Paid
    WHEN 0
     THEN '_'
    ELSE 'P'
    END
   ) + (
   CASE SA.Encoded
    WHEN 0
     THEN '_'
    ELSE 'E'
    END
   ) + (
   CASE SA.Validated
    WHEN 0
     THEN '_'
    ELSE 'V'
    END
   ) + (
   CASE SA.Completed
    WHEN 0
     THEN '_'
    ELSE 'C'
    END
   ) AS [STATUS]
  ,(
   CASE SA.StatusCode
    WHEN 1
     THEN (
       CASE 
        WHEN SA.Completed <> 0
         THEN 'Closed'
        WHEN SA.Approved = 0
         THEN 'For Approval'
          --when DATA_Lock.SessionId is not null then 'In Use' 
        ELSE 'Open'
        END
       )
    WHEN 2
     THEN 'Deleted'
    WHEN 3
     THEN 'Canceled'
    ELSE 'A'
    END
   ) AS StatusCodeExt
   ,SI.SaleItemId AS SI_SaleItemId
   ,SI.MatrixCellId AS SI_MatrixCellId
   ,CP.EventId AS cp_EventId
   ,si.saleid AS si_saleid
   ,SI.Quantity AS SI_Quantity
   ,cp.PerformanceName AS cp_PerformanceName
   ,s2p.PerformanceId AS s2p_PerformanceId
   ,cp.PerfDateTime AS cp_PerfDateTime
   ,SI.UnitPrice AS SI_UnitPrice
   ,si.UnitRawPrice AS si_UnitRawPrice
   ,SA.UserId
   ,SA.Paid
   ,SA.Completed
   
  FROM [dbo].[DATA_Sale] SA WITH (NOLOCK)
  INNER JOIN [dbo].[DATA_SaleItem] SI WITH (NOLOCK) ON SI.SaleId = SA.SaleId
  LEFT JOIN [dbo].[DATA_SaleItem2Performance] s2p WITH (NOLOCK) ON SI.SaleItemId = s2p.SaleItemId
   AND s2p.active = 1
  LEFT JOIN Performance cp ON cp.PerformanceId = s2p.PerformanceId
  WHERE 1=1
  --and cast(cp.PERFDATETIME AS DATE) >= cast(DATEADD(MONTH, - 13, GETDATE()) AS DATE)
  --and cast(cp.PERFDATETIME AS DATE) >= '2023-01-01'
  and cast(cp.PerfDateTime as date)>= cast(getdate() as date)
  ) AS SA
 INNER JOIN Event ce ON cp_EventId = ce.EventId
 LEFT JOIN Location Loc ON Loc.LocationId = ce.LocationId
 LEFT JOIN LookUp Lkp ON Lkp.LookUpTableId = 38
  AND Lkp.ItemId = ce.EventType
 LEFT JOIN LookUp LkpS ON LkpS.LookUpTableId = 40
  AND LkpS.ItemId = ce.STATUS
 LEFT JOIN [dbo].[DATA_SaleItem2Ticket] SI2T WITH (NOLOCK) ON SI2T.SaleItemId = SI_SaleItemId
 LEFT JOIN [dbo].[DATA_Ticket] TCK WITH (NOLOCK) ON TCK.TicketId = SI2T.TicketId
 LEFT JOIN TicketCatalogue TicketCatalogue WITH(NOLOCK) ON (TicketCatalogue.TicketProductId = TCK.MatrixCellId)
 LEFT JOIN MatrixCell MC ON MC.MatrixCellId = SI_MatrixCellId
 LEFT JOIN MatrixSheet MS ON MS.MatrixSheetId = MC.MatrixSheetId
 JOIN (
  SELECT *
  FROM LookUp V WITH (NOLOCK)
  WHERE LookUpTableId = 5
  ) [VOID] ON [VOID].ITEMID = TCK.VoidType
 JOIN (
  SELECT  MAX(TR.TransactionDate) AS TransactionDate
   ,max(TransactionType) AS TransactionType
   ,max(tr.paid) AS Tr_Paid_Status
   ,max(tr.Completed) AS Tr_Completed
   ,SaleId
  FROM [dbo].[DATA_Transaction] TR WITH (NOLOCK)
  --WHERE TR.TransactionDate >= cast(DATEADD(MONTH, - 13, GETDATE()) AS DATE)
  GROUP BY SaleId
  ) LastMod ON LastMod.SaleId = SA.SaleId
 LEFT JOIN [dbo].[DATA_Reservation] R WITH (NOLOCK) ON R.SaleId = SA.SaleId
 INNER JOIN EA_User U ON U.UserId = SA.UserId
 LEFT JOIN (
  SELECT DISTINCT A.SALEID
   ,A.SALEITEMID
   ,A.CODE
   ,b.matrixcellid
   ,A.CNT
   ,CASE 
    WHEN A.CNT > 1
     THEN A.PromotionCode
    ELSE B.PromotionCode
    END AS PromotionCode
   ,CASE 
    WHEN A.CNT > 1
     THEN A.PromotionDescription
    ELSE B.PromotionDescription
    END AS PromotionDescription
  FROM (
   SELECT saleid
    ,saleitemid
    ,matrixcellid
    ,code
    ,COUNT(PROMOTIONID) AS CNT
    ,STRING_AGG(PromotionDescription, ', ') AS PromotionDescription 
    ,STRING_AGG(PromotionCode, ', ') AS PromotionCode  
   FROM SaleItemPromotion
   GROUP BY SALEID
    ,SALEITEMID
    ,CODE
    ,matrixcellid
   ) A
  INNER JOIN SaleItemPromotion B ON A.SALEID = B.SALEID
   AND A.SALEITEMID = B.SALEITEMID
   AND A.CODE = B.CODE
   AND a.matrixcellid = b.matrixcellid
  ) Promo ON si_saleid = promo.saleid
  AND si_saleitemid = promo.saleitemid
  AND si_MatrixCellId = promo.MatrixCellId
 WHERE (
   SA.Paid = 1 OR LastMod.Tr_Paid_Status = 1
   )
  AND (
    SA.Completed = 1 OR LastMod.Tr_Completed = 1
   )
  AND SI_Quantity <> 0 
 ) 
 ,INTER_SALES_FACT
AS (
 SELECT DISTINCT SALEDATE
  ,LASTMODIFIEDDATE
  ,PERFDATETIME
  ,Name
  ,CODE
  ,PRODUCT_CODE
  ,PRODUCT_NAME
  ,PRODUCT_AK
  ,SALEID
  ,TICKETID
  ,RESERVATIONAK
  ,PerformanceName
  ,EventName
  ,EventCode
  ,EVENT_ID
  --,SEATING_CATEGORY_CODE
  ,PROMOTIONCODE
  ,PromotionDescription
  ,VOID_TYPE_DESC
  ,TRANSACTIONTYPE
  ,QUANTITY
  ,UNITPRICE
  ,UNITDISCOUNT
  ,PerformanceId
  ,USERNAME
  ,RoleNames
  ,StatusCodeExt
  ,TicketCode
 FROM INTER_SALES_FACT_NEW
 )
SELECT CAST(SALES.SALEDATE AS DATE) AS "AS_OF_DATE"
 ,SALES.LASTMODIFIEDDATE AS "Ticket_Sale_Date_Time"
 ,SALES.PERFDATETIME AS "Ticket_Performance_Date_Time"
 ,COALESCE(SALES.Name, 'Not Available') AS "Product_Name"
 ,SALES.CODE AS "Ticket_Code"
 ,code as code
 ,CASE 
  WHEN PATINDEX('%[0-9][0-9][0-9]P%', code) > 0
   THEN 'Platinum'
  WHEN PATINDEX('%[0-9][0-9][0-9]G%', code) > 0
   THEN 'Gold'
  WHEN PATINDEX('%[0-9][0-9][0-9]S%', code) > 0
   THEN 'Silver'
  WHEN PATINDEX('%[0-9][0-9][0-9]B%', replace(code, 'BO', '')) > 0
   THEN 'Bronze'
  WHEN PATINDEX('%[0-9][0-9]R%', code) > 0
   THEN 'Royal'
  WHEN PATINDEX('%[0-9][0-9]V%', code) > 0
   THEN 'VIP'
  WHEN PATINDEX('%[0-9][0-9]D%', code) > 0
   THEN 'Diamond'
  WHEN PATINDEX('%[0-9][0-9]F%', code) > 0
   THEN 'VIP'
  WHEN code IS NOT NULL
   THEN 'General Admission'
  ELSE 'Not Available'
  END AS "Ticket_Type"
 ,SALES.SALEID AS "Sale_ID"
 ,SALES.TICKETID AS "Ticket_ID"
 ,SALES.RESERVATIONAK AS "Reservation_ID"
 ,COALESCE(nullif(SALES.PerformanceName, ''), 'Not Available') AS "Event_Description"
 ,COALESCE(nullif(SALES.EventName, ''), 'Not Available') AS "Event_Name"
 ,EVENT_ID
 ,SALES.PROMOTIONCODE AS "OMNI_Promotion_Code"
 ,SALES.PromotionDescription AS "OMNI_Promotion_Name"
 ,SALES.VOID_TYPE_DESC AS "Void_Type"
 ,'OMNI_NEW' AS "Source_Name"
 ,CASE 
  WHEN TRANSACTIONTYPE = 115
   THEN 0
  ELSE (
    (
     CASE 
      WHEN ISNULL(SALES.QUANTITY, 0) = 0
       THEN 1
      WHEN TICKETID IS NULL
       THEN SALES.QUANTITY
      ELSE SALES.QUANTITY / SALES.QUANTITY
      END
     ) * (
     CASE 
      WHEN SALES.QUANTITY < 0
       THEN - 1
      ELSE 1
      END
     )
    )
  END AS quantity
 ,SALES.UNITPRICE AS "total_amount"
 ,SALES.UNITDISCOUNT AS "discount_amount"
 ,CAST(CURRENT_TIMESTAMP AS DATETIME) AS "record_date"
 ,SALES.PerformanceId AS "PERF_ID"
 ,SALES.USERNAME AS "USER_NAME"
 ,SALES.RoleNames AS "USER_ROLE"
 ,EA_Capacity.ArenaCapacity AS "Arena_Capacity"
 ,SEATING_CATEGORY_CODE AS "Seating_Category_Code"
 ,"Viva Product Statistical Group" AS "Viva_Product_Statistical_Group"
 ,"Viva Product Statistical Group Level 1" AS "Viva_Product_Statistical_Group_Level_1"
 ,"Viva Product Statistical Group Level 2" AS "Viva_Product_Statistical_Group_Level_2"
 ,"Viva Product Statistical Group Level 3" AS "Viva_Product_Statistical_Group_Level_3"
 ,"Viva Product Ticket Name" AS "Viva_Product_Ticket_Name"
 ,quantity_multiplier AS "Viva_Product_Quantity_Multiplier"
 ,StatusCodeExt AS "Sale_Transaction_Status"
 ,DIM_PRODUCT.SEATING_CATEGORY_CODE as "Viva_Product_Seating_Category_Code"
 ,EA_Capacity.SeatingCategoryCapacity as "Seating_Category_Capacity"
FROM Inter_Sales_Fact SALES
LEFT JOIN DIM_PRODUCT AS DIM_PRODUCT ON DIM_PRODUCT.PRODUCT_AK = SALES.PRODUCT_AK
LEFT JOIN EA_Capacity AS EA_Capacity ON EA_Capacity.PerformanceId = SALES.PerformanceId
AND EA_Capacity.SeatingCategoryCode=SEATING_CATEGORY_CODE 
