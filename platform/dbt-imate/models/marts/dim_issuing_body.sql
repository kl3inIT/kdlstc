{{ config(materialized='incremental', unique_key='body_code') }}

-- Same key discipline as dim_document_kind. The difference is that this list is
-- OPEN: the pipeline auto-registers a body the first time it appears, with
-- is_confirmed = false, and that flag travels all the way here so a report can
-- tell a verified organisation name from one the pipeline guessed from a code.

with reference as (
    select body_code, body_name, body_level, is_confirmed
      from {{ source('refdata', 'issuing_body') }}
), unknown_member as (
    select '(chua xac dinh)'::text as body_code,
           'Chưa xác định'::text   as body_name,
           null::text              as body_level,
           false                   as is_confirmed
), candidates as (
    select * from reference
    union all
    select * from unknown_member
)

{% if is_incremental() %}
select
    case when c.body_code = '(chua xac dinh)' then -1
         else (select coalesce(max(body_key), 0) from {{ this }} where body_key > 0)
              + row_number() over (order by c.body_code)
    end                        as body_key,
    c.body_code, c.body_name, c.body_level, c.is_confirmed
from candidates c
where not exists (select 1 from {{ this }} t where t.body_code = c.body_code)

union all

select t.body_key, t.body_code, c.body_name, c.body_level, c.is_confirmed
from {{ this }} t
join candidates c on c.body_code = t.body_code
where (t.body_name, coalesce(t.body_level, ''), t.is_confirmed)
   is distinct from (c.body_name, coalesce(c.body_level, ''), c.is_confirmed)

{% else %}
select
    case when c.body_code = '(chua xac dinh)' then -1
         else row_number() over (order by c.body_code) end as body_key,
    c.body_code, c.body_name, c.body_level, c.is_confirmed
from candidates c
{% endif %}
