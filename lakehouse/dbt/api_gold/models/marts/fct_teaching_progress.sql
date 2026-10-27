{{ config(alias=var('gold_alias', 'teaching_progress_metrics')) }}

select
    ma_don_vi,
    ten_don_vi,
    ma_lop_hoc_phan,
    nam_hoc,
    hoc_ky,
    ty_le_tien_do_giang_day,
    cast(1 as bigint) as so_ban_ghi_theo_doi,
    source_record_id,
    source_updated_at,
    source_batch_id,
    source_record_checksum,
    cast(current_timestamp as timestamp(6) with time zone) as _gold_updated_at
from {{ ref('stg_teaching_progress') }}
