SELECT 
    DmgCategoryId,
    DmgParentId,
    DmgCategoryAK,
    DmgCategoryCode,
    Description,
    DmgType,
    RecursiveName,
    DmgSubType,
    LinkedDmgCat
FROM dbo.CNF_DmgCategory WITH (NOLOCK)
