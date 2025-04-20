with src as (
    select * from {{ ref('stg_tickets') }}
)
select
    passenger_id::uuid as passenger_id,
    ticket_no,
    book_ref,
    passenger_id as passenger_nk,
    passenger_name,
    contact_data::jsonb ->> 'phone' as phone,
    contact_data::jsonb ->> 'email' as email,
    current_timestamp as created_at,
    current_timestamp as updated_at
from src;