{% snapshot customers_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=[
      'email',
      'name',
      'country',
      'status'
    ]
  )
}}

SELECT
    customer_id,
    email,
    name,
    country,
    status,
    created_at
FROM {{ ref('stg_customers') }}

{% endsnapshot %}
