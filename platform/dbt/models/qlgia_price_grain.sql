{{
  config(
    alias='stg_qlgia__price_grain',
    tags=['qlgia'],
    unique_key='run_id',
    incremental_strategy='delete+insert'
  )
}}

select
    run_id,
    commodity_code,
    locality_code,
    survey_period,
    min(survey_date)     filter (where is_valid)     as survey_date,
    min(unit_of_measure) filter (where is_valid)     as unit_of_measure,
    round(avg(price) filter (where is_valid), 2)::numeric(18, 2) as avg_price,
    (min(price) filter (where is_valid))::numeric(18, 2) as min_price,
    (max(price) filter (where is_valid))::numeric(18, 2) as max_price,
    (count(*) filter (where is_valid))::int as survey_points,
    (count(*) filter (where not is_valid))::int as rejected_points,
    array_agg(distinct raw_path) as raw_paths,
    current_timestamp as created_at
from {{ ref('qlgia_price_typed') }}
where run_id = '{{ var("run_id") }}'
  and commodity_code is not null
  and locality_code  is not null
group by run_id, commodity_code, locality_code, survey_period
