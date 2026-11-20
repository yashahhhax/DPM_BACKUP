SELECT
    id,
    partner_id,
    customer_id,
    date_create,
    validity_duration_month,
    validity_date,
    code,
    title,
    catalog_id,
    price_inc_tax,
    activate,
    enabled,
    invoice_id,
    price_category,
    tbx_booking_reference
from
    partner_vouchers pv
