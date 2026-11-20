SELECT
    cp.PerformanceId ,cp.PerfDateTime ,cp.PerformanceAK ,cp.PerformanceName
    ,ce.EventAK ,ce.EventCode,ce.EventName
	,SP2T.MatrixCellAk AS ProductCode
    ,cp.Status,cp.OpeningTime ,cp.ClosingTime ,cp.HasCapacity,cc.Quantity
    ,cc.BookedQuantity,cc.PaidQuantity,cc.InUseQuantity        
    ,count(tu.ticketid) as Attendance
    ,count(distinct tu.ticketid) as Attendance_Uniq

FROM CNF_Performance cp 
INNER JOIN CNF_Event ce ON cp.EventId=ce.EventId             
INNER JOIN CNF_SeatCategory cs on cp.PerformanceId=cs.PerformanceId
INNER JOIN CNF_EnvelopeCapacity cc on cs.SeatCategoryId=cc.SeatCategoryId
INNER JOIN (
	SELECT DISTINCT s2p.PerformanceId,SI2T.TicketId, mc.MatrixCellAk
	FROM DATA_SaleItem2Performance s2p  With(NoLock) 
	INNER JOIN DATA_SaleItem2Ticket SI2T With(NoLock) ON SI2T.SaleItemId = s2p.SaleItemId
	INNER JOIN DATA_SaleItem SI With(NoLock) ON SI.SaleItemID = si2t.SaleItemId
	LEFT JOIN CNF_MatrixCell mc WITH(NOLOCK) ON SI.MatrixCellId=mc.MatrixCellId
) SP2T ON cp.PerformanceId=SP2T.PerformanceId
LEFT JOIN DATA_TicketUsage tu With(NoLock) ON tu.GateId in (Select GateId FROM ADM_Gate WHERE Name like '%SRQP%') AND TU.TicketId =SP2T.TicketId 
                AND cast(CONVERT(VARCHAR(16),tu.UsageDate, 120) AS DateTime) >= cp.OpeningTime
                AND cast(CONVERT(VARCHAR(16),tu.UsageDate, 120) AS DateTime) < cp.ClosingTime
                AND TU.UsageType=1 and TU.ValidateResult < 99 
WHERE 1=1
AND ce.EventCode LIKE 'SRQP%'
GROUP BY  cp.PerformanceId ,cp.PerfDateTime ,cp.PerformanceAK ,cp.PerformanceName
        ,ce.EventAK ,ce.EventCode,ce.EventName,SP2T.MatrixCellAk
        ,cp.Status,cp.OpeningTime ,cp.ClosingTime ,cp.HasCapacity,cc.Quantity
        ,cc.BookedQuantity,cc.PaidQuantity,cc.InUseQuantity
