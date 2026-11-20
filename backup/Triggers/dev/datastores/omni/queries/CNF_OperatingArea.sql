SELECT 
    OperatingAreaId,
    SystemId,
    SiteId,
    OperatingAreaAK,
    AreaCode,
    OpAreaName,
    DefaultLevel,
    Enabled,
    DmgCategoryId
FROM dbo.CNF_OperatingArea WITH (NOLOCK)
