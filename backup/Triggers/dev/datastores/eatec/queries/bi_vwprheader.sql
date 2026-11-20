select
    [Unique Key] as unique_key
    ,[PR Number] as pr_number 
    ,[Facility Key] as facility_key
    ,[Created] as created
    ,[Created By] as created_by
    ,[Status] as status
    ,[Department] as department
    ,[Category] as category
    ,[Amount] as amount
    ,[Rejection Reason] as rejection_reason
    ,[Created DateTime] as created_datetime
from dbo.bi_vwprheader WITH (NOLOCK)
