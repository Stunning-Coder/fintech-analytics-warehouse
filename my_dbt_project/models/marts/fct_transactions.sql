-- models/marts/fct_transactions.sql
-- desc: creates a single, wide table of all transactions, enriched with the user_id for analysis

-- This output will be big, so I will materialize it as a table
{{ config(materialized='table') }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
)

select
    tx.transaction_id,
    tx.account_id,
    -- connects this fact to the user dimension
    acct.user_id,
    tx.transaction_at,
    tx.transaction_date,
    tx.transaction_type,
    tx.amount
from
    transactions as tx
left join
    accounts as acct on tx.account_id = acct.account_id
