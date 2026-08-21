{{ config(materialized='incremental', unique_key='kind_code') }}

-- Surrogate keys are assigned above the current maximum and NEVER renumbered.
-- Renumbering is silent corruption: every fact row keeps its old integer and
-- now points at a different kind, and nothing errors.
--
-- That is also why this model is incremental rather than a table. A full rebuild
-- would reassign every key on every run.

with reference as (
    select kind_code, kind_name, kind_group, sort_order
      from {{ source('refdata', 'document_kind') }}
), unknown_member as (
    -- A fact row must never be dropped for want of a dimension row.
    select '(chua xac dinh)'::text as kind_code,
           'Chưa xác định'::text   as kind_name,
           null::text              as kind_group,
           999::int                as sort_order
), candidates as (
    select * from reference
    union all
    select * from unknown_member
)

{% if is_incremental() %}
select
    case when c.kind_code = '(chua xac dinh)' then -1
         else (select coalesce(max(kind_key), 0) from {{ this }} where kind_key > 0)
              + row_number() over (order by c.kind_code)
    end                        as kind_key,
    c.kind_code, c.kind_name, c.kind_group, c.sort_order
from candidates c
where not exists (select 1 from {{ this }} t where t.kind_code = c.kind_code)

union all

-- Names may have been confirmed since the last publish; keys stay put, names follow.
select t.kind_key, t.kind_code, c.kind_name, c.kind_group, c.sort_order
from {{ this }} t
join candidates c on c.kind_code = t.kind_code
where (t.kind_name, coalesce(t.kind_group, ''), t.sort_order)
   is distinct from (c.kind_name, coalesce(c.kind_group, ''), c.sort_order)

{% else %}
select
    case when c.kind_code = '(chua xac dinh)' then -1
         else row_number() over (order by c.kind_code) end as kind_key,
    c.kind_code, c.kind_name, c.kind_group, c.sort_order
from candidates c
{% endif %}
