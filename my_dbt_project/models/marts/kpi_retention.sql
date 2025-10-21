-- models/marts/kpi_retention.sql

{{ config(materialized='table') }}

-- Step 1: Get the cohort (signup month) for each user
with user_cohorts as (
    select
        user_id,
        date_trunc('month', created_at) as cohort_month
    from {{ ref('dim_users') }}
),

-- Step 2: Get all monthly activity for each user
user_monthly_activity as (
    select distinct
        user_id,
        date_trunc('month', transaction_at) as activity_month
    from {{ ref('fct_transactions') }}
),

-- Step 3: Join activity to cohorts to find the "month number" for each activity
activity_relative_to_cohort as (
    select
        a.user_id,
        c.cohort_month,
        -- calculates the "month number" (e.g., 0, 1, 2...)
        {{ dbt.datediff('c.cohort_month', 'a.activity_month', 'month') }} as month_number
    from user_monthly_activity as a
    left join user_cohorts as c
        on a.user_id = c.user_id
    where a.activity_month >= c.cohort_month -- Only count activity *after* signup
),

-- Step 4: Count the number of unique users retained for each cohort-month pair
cohort_retention as (
    select
        cohort_month,
        month_number,
        count(distinct user_id) as retained_users
    from activity_relative_to_cohort
    group by 1, 2
),

-- Step 5: Get the original size of each cohort
cohort_size as (
    select
        cohort_month,
        count(distinct user_id) as total_users
    from user_cohorts
    group by 1
)

-- Final Step: Join size and retention counts to get the percentage
select
    s.cohort_month,
    c.month_number,
    s.total_users,
    c.retained_users,
    -- Use ::float for accurate division in Postgres
    (c.retained_users::float / s.total_users::float) as retention_percentage
from cohort_retention as c
left join cohort_size as s
    on c.cohort_month = s.cohort_month
order by
    1, 2
