{{ config(alias=var('learning_period_alias', 'learning_outcomes_by_period')) }}

with totals as (
    select nam_hoc, hoc_ky,
        count(distinct ma_chuong_trinh) as so_chuong_trinh,
        sum(so_luot_hoc_phan_dat) as so_luot_hoc_phan_dat,
        sum(tong_luot_hoc_phan) as tong_luot_hoc_phan,
        sum(tong_diem_gpa) as tong_diem_gpa,
        sum(so_sinh_vien_tinh_gpa) as so_sinh_vien_tinh_gpa,
        sum(so_sinh_vien_dung_tien_do) as so_sinh_vien_dung_tien_do,
        sum(so_sinh_vien_danh_gia_tien_do) as so_sinh_vien_danh_gia_tien_do
    from {{ ref('fct_learning_outcomes') }}
    group by nam_hoc, hoc_ky
)
select nam_hoc, hoc_ky, so_chuong_trinh,
    so_luot_hoc_phan_dat, tong_luot_hoc_phan,
    case when tong_luot_hoc_phan > 0 then cast(so_luot_hoc_phan_dat as double) * 100.0 / cast(tong_luot_hoc_phan as double) end as ty_le_qua_hoc_phan,
    tong_diem_gpa, so_sinh_vien_tinh_gpa,
    case when so_sinh_vien_tinh_gpa > 0 then cast(tong_diem_gpa as double) / cast(so_sinh_vien_tinh_gpa as double) end as gpa_trung_binh,
    so_sinh_vien_dung_tien_do, so_sinh_vien_danh_gia_tien_do,
    case when so_sinh_vien_danh_gia_tien_do > 0 then cast(so_sinh_vien_dung_tien_do as double) * 100.0 / cast(so_sinh_vien_danh_gia_tien_do as double) end as ty_le_dung_tien_do,
    cast(current_timestamp as timestamp(6) with time zone) as _gold_updated_at
from totals
