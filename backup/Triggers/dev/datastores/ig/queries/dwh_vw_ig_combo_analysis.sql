SELECT 
oh.close_dttime TENDERED_DATE_TIME,
oh.check_no CHECK_NUMBER,
oh.assoc_check_no ASSOC_CHECK_NUMBER,
oi.menu_item_id MENU_ITEM_ID,
mim.menu_item_name MENU_ITEM_NAME,
oi.package_id PACKAGE_ID,
oi.discount_amt DISCOUNT_AMT,
oi.item_qty ITEM_QTY,
oi.sales_amt_gross SALES_AMT_GROSS,
oi.tax_amt_incl_sales TAX_AMT_INCL_SALES
FROM [it_trn]..[Order_Header] oh WITH (NOLOCK)
LEFT JOIN [it_trn]..[Order_Item] oi WITH (NOLOCK) ON oh.order_hdr_id = oi.order_hdr_id
LEFT JOIN [it_cfg]..[Menu_Item_Master] mim WITH (NOLOCK) on oi.menu_item_id = mim.menu_item_id
where CAST(oh.close_dttime AS DATE) >= DATEADD(day, -7, CAST(GETDATE() AS date))
