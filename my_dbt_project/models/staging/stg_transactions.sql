-- models/staging/stg_transactions.sql
-- Purpose: Cleans and standardizes raw transaction data for downstream modeling

select
    transaction_id,
    account_id,
    
    -- Rename and standardize timestamp for clarity
    "timestamp" as transaction_at,
    
    -- Extract transaction_date for easier joins to date dimensions or time-based aggregation
    CAST("timestamp" AS DATE) as transaction_date,
    
    amount,
    type as transaction_type
from
    {{ source('raw_data', 'raw_transactions') }}

