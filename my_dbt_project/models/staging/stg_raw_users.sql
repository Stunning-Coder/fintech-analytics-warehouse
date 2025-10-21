-- models/staging/stg_raw_users.sql

/*
  Staging model for raw_users.
  This model selects and lightly cleans user data from the raw source table.
  Note: The 'source' macro below references the 'raw_users' table defined in 
sources.yml.
*/

select
    user_id,
    created_at,
    email,
    status
from {{ source('raw_data', 'raw_users') }}

