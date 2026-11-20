SELECT 
    StatisticalGroupId,
    StatisticalGroupCode,
    StatisticalGroupDescription,
    ParentId,
    Position,
    Level,
    Enabled,
    StatisticalGroupAK,
    DmgCategoryId
FROM dbo.CNF_StatisticalGroup WITH (NOLOCK)
