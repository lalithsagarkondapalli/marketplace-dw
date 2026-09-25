select
    order_id,
    {{ to_int('payment_sequential') }}   as payment_seq,
    lower(trim(payment_type))            as payment_type,
    {{ to_int('payment_installments') }} as payment_installments,
    {{ to_num('payment_value') }}        as payment_value,
    _loaded_at
from {{ source('raw', 'order_payments') }}
