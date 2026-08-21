{{ config(materialized='incremental', unique_key=['global_id', 'seq'],
          incremental_strategy='delete+insert') }}

-- One row per hop a document made. Grain is deliberately finer than
-- fact_document — see the note there.
--
-- receiver is polymorphic in the source: '#<contactId>' when it is a person,
-- and a plain unit name otherwise. Rather than guess, the three outcomes are
-- named: contact, unit, unknown. 'unknown' is stored as a value, not left null,
-- because a null reads as "not applicable" while this means "we could not
-- resolve it" — measured at 429 rows out of 99.214, and that number should be
-- visible rather than inferred from a gap.

select
    r.global_id,
    r.seq,
    to_char(t.uploaded_date, 'YYYYMMDD')::int  as date_key,
    r.sender                                   as sender_handle,
    case when r.sender like '#%' then substr(r.sender, 2) end
                                               as sender_contact_id,
    r.receiver                                 as receiver_handle,
    case when r.receiver like '#%' then 'contact'
         when exists (select 1 from {{ source('refdata', 'imate_unit') }} u
                       where u.unit_name = r.receiver) then 'unit'
         else 'unknown' end                    as receiver_kind,
    case when r.receiver like '#%' then substr(r.receiver, 2) end
                                               as receiver_contact_id,
    case when r.receiver not like '#%' then r.receiver end
                                               as receiver_unit_name,
    r.action,
    r.role,
    case when r.seen_at  ~ '^\d{4}-\d{2}-\d{2}T' then r.seen_at::timestamptz  end as seen_at,
    case when r.acted_at ~ '^\d{4}-\d{2}-\d{2}T' then r.acted_at::timestamptz end as acted_at,
    t.run_id
from {{ source('staging', 'stg_imate__routing') }} r
join {{ source('staging', 'stg_imate__document_typed') }} t
  on t.global_id = r.global_id
where t.defect_reason is null
  and t.uploaded_date is not null
