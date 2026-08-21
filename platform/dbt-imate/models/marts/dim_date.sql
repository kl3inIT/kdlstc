{{ config(materialized='table', unique_key='date_key') }}

-- Every day between the first and last document EXISTS as a row, including days
-- with nothing in them. A report that joins from here shows an empty day as a
-- zero; a report that joins from the facts shows nothing at all, and a missing
-- row reads as "no data yet" when it means "nobody sent anything".
--
-- Rebuilt in full each run rather than incrementally: date_key is derived from
-- the date itself, so a rebuild produces byte-identical keys. Nothing downstream
-- can be re-pointed by it.

with bounds as (
    select min(uploaded_date) as from_date,
           max(uploaded_date) as to_date
      from {{ source('staging', 'stg_imate__document_typed') }}
     where uploaded_date is not null
), calendar as (
    select generate_series(b.from_date, b.to_date, interval '1 day')::date as d
      from bounds b
)
select
    to_char(d, 'YYYYMMDD')::int          as date_key,
    d                                    as full_date,
    extract(year    from d)::int         as year,
    extract(quarter from d)::int         as quarter,
    extract(month   from d)::int         as month,
    to_char(d, 'YYYYMM')::int            as month_key,
    'Tháng ' || to_char(d, 'MM/YYYY')    as month_label,
    extract(day     from d)::int         as day_of_month,
    extract(isodow  from d)::int         as day_of_week,
    to_char(d, 'DD/MM/YYYY')             as day_label,
    extract(isodow from d) in (6, 7)     as is_weekend
from calendar
