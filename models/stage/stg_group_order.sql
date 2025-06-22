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
    user_id,
    status,
    count(*) as completed_order_count,
    {{var('load_date')}} as LOAD_DATE,
    {{ var('load_date') }} AS EFFECTIVE_FROM
from staging
where status = 'completed'
group by user_id, status
order by completed_order_count, user_id desc