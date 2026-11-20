SELECT *
FROM Vw_InPark_Attendance_Detail
WHERE UsageDate >= CAST(GETDATE() - 1 AS DATE)
