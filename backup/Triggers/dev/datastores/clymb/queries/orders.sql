SELECT
    id,
    customer_id,
    user_id,
    date_create,
    num,
    origin,
    status,
    efpos,
    total_exc_tax,
    total_inc_tax,
    fort_id,
    authorization_code,
    last_update
from
    orders o
