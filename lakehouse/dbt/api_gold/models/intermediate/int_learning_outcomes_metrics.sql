{{ config(materialized='ephemeral') }}
select *,
    cast(nullif(regexp_extract(nam_hoc, '^(\d{4})-', 1), '') as integer) as nam_bat_dau,
    case when tong_luot_hoc_phan > 0 then cast(so_luot_hoc_phan_dat as double) * 100.0 / cast(tong_luot_hoc_phan as double) end as ty_le_qua_hoc_phan,
    case when so_sinh_vien_tinh_gpa > 0 then cast(tong_diem_gpa as double) / cast(so_sinh_vien_tinh_gpa as double) end as gpa_trung_binh,
    case when so_sinh_vien_danh_gia_tien_do > 0 then cast(so_sinh_vien_dung_tien_do as double) * 100.0 / cast(so_sinh_vien_danh_gia_tien_do as double) end as ty_le_dung_tien_do
from {{ ref('stg_learning_outcomes') }}
