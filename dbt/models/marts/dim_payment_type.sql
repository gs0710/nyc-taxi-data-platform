select
    payment_type        as payment_type_key,
    payment_type_name
from {{ ref('payment_type_codes') }}
