-- models/staging/stg_accounts.sql

select
    account_id,
    user_id,
    account_type,
    opened_at
from
    {{ source('raw_data', 'raw_accounts') }}
