select
	UsageDate,
	SiteId,
	SiteName,
	sum(KPI_Entry) KPI_Entry,
	sum(KPI_Reentry) KPI_Reentry,
	sum(KPI_CrossOver) KPI_CrossOver,
	sum(KPI_Exit) KPI_Exit,
	sum(KPI_Entry_AAZ_Migrated) KPI_Entry_AAZ_Migrated
from Vw_InPark_Attendance_Summary
where UsageDate >= CAST(GETDATE() - 1 AS DATE)
Group by UsageDate, SiteId, SiteName
