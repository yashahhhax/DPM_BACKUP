 select PerformanceId
 ,SeatingCategoryCode,ticket_type,EVENTID ,EVENTCODE ,Performancename 
 ,sum(ArenaCapacity) as SeatingCategoryCapacity
 from (
 select 
 cp.PerformanceId
 ,csc.code as SeatingCategoryCode
 ,cec.Quantity as ArenaCapacity
 ,ce.EVENTID ,ce.EVENTCODE ,cp.Performancename,CASE 
  WHEN PATINDEX('%[0-9][0-9][0-9]P%', csc.code) > 0
   THEN 'Platinum'
  WHEN PATINDEX('%[0-9][0-9][0-9]G%', csc.code) > 0
   THEN 'Gold'
  WHEN PATINDEX('%[0-9][0-9][0-9]S%', csc.code) > 0
   THEN 'Silver'
  WHEN PATINDEX('%[0-9][0-9][0-9]B%', replace(csc.code, 'BO', '')) > 0
   THEN 'Bronze'
  WHEN PATINDEX('%[0-9][0-9]R%', csc.code) > 0
   THEN 'Royal'
  WHEN PATINDEX('%[0-9][0-9]V%', csc.code) > 0
   THEN 'VIP'
  WHEN PATINDEX('%[0-9][0-9]D%', csc.code) > 0
   THEN 'Diamond'
  WHEN PATINDEX('%[0-9][0-9]F%', csc.code) > 0
   THEN 'VIP'
  WHEN csc.code IS NOT NULL
   THEN 'General Admission'
  ELSE 'Not Available'
  END AS  ticket_type
 from
 CNF_Performance cp
 join cnf_event ce on cp.EventId=ce.EventId
 join CNF_SeatCategory csc on csc.PerformanceId=cp.PerformanceId
 join CNF_EnvelopeCapacity cec on cec.SeatCategoryId=csc.SeatCategoryId
 join CNF_Location Loc on Loc.LocationId=ce.LocationId
 join CNF_SpaceStructure sst on cec.SpaceStructureId= sst.SpaceStructureId and cp.PerformanceId=sst.PerformanceId
 join CNF_Envelope env on cec.EnvelopeId=env.EnvelopeId
 where env.Code not in ('SD','KILL','BLOCK') --and cast(cp.PerfDateTime as date)  >= cast(getDate() as date)
 ) x group by PerformanceId,SeatingCategoryCode,ticket_type,EVENTID ,EVENTCODE ,Performancename 
