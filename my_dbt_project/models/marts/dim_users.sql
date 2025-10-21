-- models/marts/dim_users.sql

/*
  Dimensional model: dim_users
  Combines user base info, profiles, KYC status, and marketing attribution.
  Grain: one row per user_id
*/

with
users as (
    select
        user_id,
        created_at,
        email,
        status as user_status
    from {{ source('raw_data', 'raw_users') }}
),

profiles as (
    select
        user_id,
        first_name,
        last_name,
        dob
    from {{ source('raw_data', 'raw_user_profiles') }}
),

kyc as (
    select
        user_id,
        status as kyc_status,
        checked_at as kyc_checked_at
    from {{ source('raw_data', 'raw_kyc_checks') }}
),

attribution as (
    select
        user_id,
        signup_source,
        campaign
    from {{ source('raw_data', 'raw_marketing_attribution') }}
),

final as (
    select
        u.user_id,
        u.created_at,
        u.email,
        u.user_status,
        p.first_name,
        p.last_name,
        p.dob,
        k.kyc_status,
        k.kyc_checked_at,
        a.signup_source,
        a.campaign
    from users u
    left join profiles p on u.user_id = p.user_id
    left join kyc k on u.user_id = k.user_id
    left join attribution a on u.user_id = a.user_id
)

select * from final

