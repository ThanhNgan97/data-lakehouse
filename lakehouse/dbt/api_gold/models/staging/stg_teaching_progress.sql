-- Preserve source values and types. Silver owns cleaning and deduplication.
select
    ma_don_vi,
    ten_don_vi,
    ma_lop_hoc_phan,
    nam_hoc,
    hoc_ky,
    ty_le_tien_do_giang_day,
    ma_ban_ghi as source_record_id,
    thoi_gian_cap_nhat_nguon as source_updated_at,
    _batch_id as source_batch_id,
    _record_checksum as source_record_checksum
from {{ source('silver', 'teaching_progress') }}
