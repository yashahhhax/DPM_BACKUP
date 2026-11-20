SELECT
    CurrencyId,
    SystemId,
    ISOCode,
    ISOCodeNumeric,
    CurrencyType,
    Symbol,
    PosOnDisplay,
    Name,
    Format,
    ExchDecimals,
    ExchangeRate,
    RoundDecimals,
    RoundingRuleId,
    Last_UserId_Log,
    Last_WorkstationId_Log,
    Last_DateTime_Log
FROM dbo.CNF_Currency WITH (NOLOCK)
