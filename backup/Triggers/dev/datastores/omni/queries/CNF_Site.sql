SELECT
    SiteId,
    SystemId,
    SiteAK,
    SiteCode,
    SiteName,
    SiteDescription,
    Enabled,
    DmgCategoryId,
    ProdWebAddress
FROM dbo.CNF_Site WITH (NOLOCK)
