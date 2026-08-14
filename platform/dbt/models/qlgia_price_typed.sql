{{
  config(
    alias='stg_qlgia__price_typed',
    tags=['qlgia'],
    unique_key='run_id',
    incremental_strategy='delete+insert'
  )
}}

with parsed as (
    select
        s.run_id,
        s.commodity_code as src_item_code,
        s.locality_code  as src_area_code,
        s.survey_period,
        s.outlet_code,
        s.source_row_id,
        s.raw_path,
        s.unit_of_measure,
        case when s.survey_date ~ '^\d{4}-\d{2}-\d{2}$'
             then s.survey_date::date end                  as survey_date,
        case when s.price ~ '^-?\d+(\.\d+)?$'
             then s.price::numeric end                     as price,
        case when s.source_updated_at ~ '^\d{4}-\d{2}-\d{2}T'
             then s.source_updated_at::timestamptz end     as source_updated_at
    from {{ source('staging', 'stg_qlgia__price') }} s
    where s.run_id = '{{ var("run_id") }}'
),

mapped as (
    select
        p.*,
        mc.standard_value as commodity_code,
        ml.standard_value as locality_code
    from parsed p
    left join {{ source('metadata', 'mapping_rules') }} mc
           on mc.source_code  = 'qlgia'
          and mc.code_type    = 'commodity'
          and mc.source_value = p.src_item_code
          and mc.valid_to     >= current_date
    left join {{ source('metadata', 'mapping_rules') }} ml
           on ml.source_code  = 'qlgia'
          and ml.code_type    = 'locality'
          and ml.source_value = p.src_area_code
          and ml.valid_to     >= current_date
),

deduped as (
    select distinct on (
        src_item_code,
        coalesce(src_area_code, '~null~'),
        survey_period,
        coalesce(outlet_code, '~null~')
    ) *
    from mapped
    order by src_item_code,
             coalesce(src_area_code, '~null~'),
             survey_period,
             coalesce(outlet_code, '~null~'),
             source_updated_at desc nulls last,
             source_row_id     desc
),

reference_price as (
    select src_item_code,
           percentile_cont(0.5) within group (order by price) as median_price
    from mapped
    where price > 0
      and src_area_code is distinct from 'QLG-TONG'
    group by src_item_code
),

judged as (
    select
        d.*,
        case
            when d.src_area_code = 'QLG-TONG'      then 'subtotal_row'
            when d.src_area_code is null           then 'locality_missing'
            when d.outlet_code is null             then 'outlet_missing'
            when d.survey_date is null             then 'survey_date_unparseable'
            when d.price is null                   then 'price_unparseable'
            when d.price <= 0                      then 'price_not_positive'
            when d.commodity_code is null          then 'commodity_unmapped'
            when d.locality_code  is null          then 'locality_unmapped'
            when r.median_price is not null
             and d.price > 20 * r.median_price     then 'price_out_of_range'
        end as reject_reason
    from deduped d
    left join reference_price r on r.src_item_code = d.src_item_code
)

select
    run_id,
    commodity_code,
    locality_code,
    survey_period,
    survey_date,
    outlet_code,
    unit_of_measure,
    price::numeric(18, 2) as price,
    source_updated_at,
    src_item_code,
    src_area_code,
    source_row_id,
    raw_path,
    reject_reason is null as is_valid,
    reject_reason,
    current_timestamp as created_at
from judged
