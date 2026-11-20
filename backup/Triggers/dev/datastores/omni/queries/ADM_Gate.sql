SELECT
    GateId,
    GateAK,
    Name,
    Description,
    LocationId,
    AccessFilterType
FROM
    dbo.ADM_Gate WITH (NOLOCK)
