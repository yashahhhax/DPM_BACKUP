SELECT a.*,b.StatusMessage AS StatusDesc,c.SiteCode,c.SiteName 
FROM [dbo].[loyalty_promotion] a 
LEFT JOIN Master_Status b ON a.StatusCode=b.StatusCode
LEFT JOIN Master_Site c ON a.SiteId=c.SiteId
