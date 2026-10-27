select g.source_record_id
from {{ ref('fct_learning_outcomes') }} g
left join (select * from {{ source('silver', 'learning_outcomes') }} where mod(crc32(to_utf8(coalesce(ma_ban_ghi, ''))), 4) = 0) s on g.source_record_id=s.ma_ban_ghi
where mod(crc32(to_utf8(coalesce(g.source_record_id, ''))), 4) = 0 AND (s.ma_ban_ghi is null or s.da_xoa is distinct from false
   or g.source_record_id is distinct from s.ma_ban_ghi
   or g.source_updated_at is distinct from s.thoi_gian_cap_nhat_nguon
   or g.source_batch_id is distinct from s._batch_id
   or g.source_record_checksum is distinct from s._record_checksum
   or g.ma_chuong_trinh is distinct from s.ma_chuong_trinh
   or g.ten_chuong_trinh is distinct from s.ten_chuong_trinh
   or g.nam_hoc is distinct from s.nam_hoc
   or g.hoc_ky is distinct from s.hoc_ky
   or g.so_sinh_vien is distinct from s.so_sinh_vien
   or g.so_luot_hoc_phan_dat is distinct from s.so_luot_hoc_phan_dat
   or g.tong_luot_hoc_phan is distinct from s.tong_luot_hoc_phan
   or g.tong_diem_gpa is distinct from s.tong_diem_gpa
   or g.so_sinh_vien_tinh_gpa is distinct from s.so_sinh_vien_tinh_gpa
   or g.so_sinh_vien_canh_bao is distinct from s.so_sinh_vien_canh_bao
   or g.so_sinh_vien_nguy_co_nghi_hoc is distinct from s.so_sinh_vien_nguy_co_nghi_hoc
   or g.so_sinh_vien_dung_tien_do is distinct from s.so_sinh_vien_dung_tien_do
   or g.so_sinh_vien_danh_gia_tien_do is distinct from s.so_sinh_vien_danh_gia_tien_do)
