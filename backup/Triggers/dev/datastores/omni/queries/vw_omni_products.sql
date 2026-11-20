select
cms.MatrixSheetName as TYPE,
ce.EventCode AS STORE,Case when (cmc.Enabled=0) then 'INACTIVE' else 'ACTIVE' end STATUS,
cpl.PriceListName as PRICELIST,
cmc.MatrixCellId,
cmc.AttributeSetId,
cas.AttributeNames,
aaa.Name,
cmc.Code as PRODUCT_CODE,
cmc.Name as PRODUCT_NAME,cmc.Price as UNIT_PRICE,cmc.MatrixCellAk as PRODUCT_AK,
cmc.CreationDate as CREATION_DATE,
cmc.LastUpdDate as LAST_MODIFIED
from CNF_MatrixCell cmc
inner join CNF_MatrixSheet cms on cms.MatrixSheetId=cmc.matrixsheetid
inner join (select * from CNF_Event where EventType=3) ce on ce.eventid=cms.EventId
inner join CNF_PriceList cpl on cpl.PriceListId=cmc.PriceListId
inner join CNF_AttributeSet cas on cas.AttributeSetId=cmc.AttributeSetId
left join (select casd.AttributeSetId,ca.Name from CNF_AttributeSetDetail casd
inner join CNF_Attribute ca on ca.AttributeId=casd.AttributeId
where ca.Name like '%Bucket%') aaa on aaa.AttributeSetId=cas.AttributeSetId;
