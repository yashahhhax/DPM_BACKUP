SELECT
    SeatCategoryId,
    Name,
    Color,
    AttributeId,
    Code,
    ExternalReference,
    PerformanceId,
    Temp_SeatCategoryId,
    Weight,
    SeatCategoryType,
    Enabled
FROM dbo.CNF_SeatCategory WITH (NOLOCK)
