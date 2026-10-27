-- Null-safe comparisons also catch missing rows and changed business values.
select g.source_record_id
from {{ ref('fct_teaching_progress') }} g
left join {{ source('silver', 'teaching_progress') }} s
    on g.source_record_id = s.ma_ban_ghi
where s.ma_ban_ghi is null
   or g.source_updated_at is distinct from s.thoi_gian_cap_nhat_nguon
   or g.source_batch_id is distinct from s._batch_id
   or g.source_record_checksum is distinct from s._record_checksum
   or g.ma_don_vi is distinct from s.ma_don_vi
   or g.ten_don_vi is distinct from s.ten_don_vi
   or g.ma_lop_hoc_phan is distinct from s.ma_lop_hoc_phan
   or g.nam_hoc is distinct from s.nam_hoc
   or g.hoc_ky is distinct from s.hoc_ky
   or g.ty_le_tien_do_giang_day is distinct from s.ty_le_tien_do_giang_day
