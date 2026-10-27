select
    ma_chuong_trinh,
    ten_chuong_trinh,
    nam_hoc,
    hoc_ky,
    so_sinh_vien,
    so_luot_hoc_phan_dat,
    tong_luot_hoc_phan,
    tong_diem_gpa,
    so_sinh_vien_tinh_gpa,
    so_sinh_vien_canh_bao,
    so_sinh_vien_nguy_co_nghi_hoc,
    so_sinh_vien_dung_tien_do,
    so_sinh_vien_danh_gia_tien_do,
    ma_ban_ghi as source_record_id,
    thoi_gian_cap_nhat_nguon as source_updated_at,
    _batch_id as source_batch_id,
    _record_checksum as source_record_checksum
from {{ source('silver', 'learning_outcomes') }}
where da_xoa = false
