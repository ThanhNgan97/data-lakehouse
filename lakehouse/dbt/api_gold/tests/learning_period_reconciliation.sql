with totals as (
select nam_hoc, hoc_ky, count(distinct ma_chuong_trinh) as so_chuong_trinh,
    sum(so_luot_hoc_phan_dat) as so_luot_hoc_phan_dat,
    sum(tong_luot_hoc_phan) as tong_luot_hoc_phan,
    sum(tong_diem_gpa) as tong_diem_gpa,
    sum(so_sinh_vien_tinh_gpa) as so_sinh_vien_tinh_gpa,
    sum(so_sinh_vien_dung_tien_do) as so_sinh_vien_dung_tien_do,
    sum(so_sinh_vien_danh_gia_tien_do) as so_sinh_vien_danh_gia_tien_do
from {{ source('silver', 'learning_outcomes') }} where da_xoa=false
group by nam_hoc,hoc_ky
), expected as (
select *,
    case when tong_luot_hoc_phan>0 then cast(so_luot_hoc_phan_dat as double)*100.0/cast(tong_luot_hoc_phan as double) end as ty_le_qua_hoc_phan,
    case when so_sinh_vien_tinh_gpa>0 then cast(tong_diem_gpa as double)/cast(so_sinh_vien_tinh_gpa as double) end as gpa_trung_binh,
    case when so_sinh_vien_danh_gia_tien_do>0 then cast(so_sinh_vien_dung_tien_do as double)*100.0/cast(so_sinh_vien_danh_gia_tien_do as double) end as ty_le_dung_tien_do
from totals
)
select coalesce(g.nam_hoc,e.nam_hoc) as nam_hoc
from {{ ref('agg_learning_outcomes_by_period') }} g
full outer join expected e on g.nam_hoc=e.nam_hoc and g.hoc_ky=e.hoc_ky
where g.nam_hoc is null or e.nam_hoc is null
   or g.so_chuong_trinh is distinct from e.so_chuong_trinh
   or g.so_luot_hoc_phan_dat is distinct from e.so_luot_hoc_phan_dat
   or g.tong_luot_hoc_phan is distinct from e.tong_luot_hoc_phan
   or g.so_sinh_vien_tinh_gpa is distinct from e.so_sinh_vien_tinh_gpa
   or g.so_sinh_vien_dung_tien_do is distinct from e.so_sinh_vien_dung_tien_do
   or g.so_sinh_vien_danh_gia_tien_do is distinct from e.so_sinh_vien_danh_gia_tien_do
   or ((g.tong_diem_gpa is null) <> (e.tong_diem_gpa is null)) or is_nan(g.tong_diem_gpa)
   or abs(g.tong_diem_gpa-e.tong_diem_gpa)>1e-10 * greatest(1.0,abs(e.tong_diem_gpa))
   or ((g.ty_le_qua_hoc_phan is null) <> (e.ty_le_qua_hoc_phan is null)) or is_nan(g.ty_le_qua_hoc_phan)
   or abs(g.ty_le_qua_hoc_phan-e.ty_le_qua_hoc_phan)>1e-10 * greatest(1.0,abs(e.ty_le_qua_hoc_phan))
   or ((g.gpa_trung_binh is null) <> (e.gpa_trung_binh is null)) or is_nan(g.gpa_trung_binh)
   or abs(g.gpa_trung_binh-e.gpa_trung_binh)>1e-10 * greatest(1.0,abs(e.gpa_trung_binh))
   or ((g.ty_le_dung_tien_do is null) <> (e.ty_le_dung_tien_do is null)) or is_nan(g.ty_le_dung_tien_do)
   or abs(g.ty_le_dung_tien_do-e.ty_le_dung_tien_do)>1e-10 * greatest(1.0,abs(e.ty_le_dung_tien_do))
