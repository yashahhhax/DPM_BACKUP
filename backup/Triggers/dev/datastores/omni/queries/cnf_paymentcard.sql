SELECT 
    PaymentCardId,
    DeviceId,
    CCCode,
    CCName,
    CCDescription,
    ValidFrom,
    ValidTo,
    FloorLimit,
    FloorLimitLocal,
    PaymentCardType,
    DefaultLevel,
    COAAccountId,
    Last_UserId_Log,
    Last_WorkstationId_Log,
    Last_DateTime_Log
FROM dbo.CNF_PaymentCard WITH (NOLOCK)
