{{ config(materialized='incremental', unique_key='global_id',
          incremental_strategy='delete+insert') }}

-- One row per document. COUNT(*) here IS the report — which is exactly why
-- routing lives in its own model at its own grain: joining ~16 routing rows in
-- without an aggregate multiplies every total by sixteen.
--
-- Load mode is merge-upsert by natural key, the third mode on this platform.
-- An iMate document is a LIVING record: the source may edit one years later, and
-- the warehouse has to end up with one row either way. qlgia appends by cursor,
-- tabmis replaces by period, imate replaces by document.
--
-- Documents carrying a structural defect are excluded here and parked in the
-- work list with their reason. Excluded is not dropped: they stay countable.

select
    t.global_id,
    t.document_id,
    t.document_no,
    t.subject,
    to_char(t.uploaded_date, 'YYYYMMDD')::int              as date_key,
    coalesce(k.kind_key, -1)                               as kind_key,
    coalesce(b.body_key, -1)                               as body_key,
    t.uploaded_at,
    t.process_status,
    t.receipt_type,
    t.routing_count,
    t.attachment_count,
    t.run_id,
    '{{ invocation_id }}'                                  as batch_id
from {{ source('staging', 'stg_imate__document_typed') }} t
left join {{ ref('dim_document_kind') }} k on k.kind_code = t.kind_code
left join {{ ref('dim_issuing_body')  }} b on b.body_code = t.body_code
where t.defect_reason is null
  and t.uploaded_date is not null
