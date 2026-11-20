SELECT
    id,
    order_id,
    item_name,
    item_type,
    item_catalog_id,
    qty,
    discount_code_id,
    discount,
    total_inc_tax,
    total_exc_tax,
    vat_rate
from
    orders_items oi
