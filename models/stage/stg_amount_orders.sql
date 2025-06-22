{{
    config(enabled=True)
}}

{%- set yaml_metadata -%}
source_model: 'stg_orders'
derived_columns:
    RECORD_SOURCE: '!CSV_ORDERS'
hashed_columns:
    LINK_ORDER_PK:
        - 'USER_ID'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

with staging as (
    {{ automate_dv.stage(include_source_columns=true,
                    source_model=source_model,
                    derived_columns=derived_columns,
                    hashed_columns=hashed_columns,
                    ranked_columns=none)
    }}
)

select 
    DATE_TRUNC('week', order_date) AS week_start,
    status,
    count(*) as order_amount,
    count(distinct user_id) as customer_amount,
    {{ var('load_date') }} as LOAD_DATE,
    {{ var('load_date') }} AS EFFECTIVE_FROM
from staging
group by DATE_TRUNC('week', order_date), status
order by  DATE_TRUNC('week', order_date) desc,order_amount desc