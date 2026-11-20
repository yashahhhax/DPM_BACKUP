SELECT 
    MatrixSheetId,
    EventId,
    MatrixSheetName,
    ProductType,
    Color,
    Position,
    Enabled,
    FlatSheet,
    SortOrder,
    MatrixSheetType,
    Code,
    IsSeasonal,
    MatrixSheetAK
FROM dbo.CNF_MatrixSheet WITH (NOLOCK)
