{{ config(materialized='ephemeral') }}
with metrics as (
    select * from {{ ref('int_learning_outcomes_metrics') }}
), narrow_periods as (
    select ma_chuong_trinh, nam_bat_dau, hoc_ky,
        lag(nam_hoc) over (partition by ma_chuong_trinh order by nam_bat_dau, hoc_ky) as nam_hoc_truoc,
        lag(hoc_ky) over (partition by ma_chuong_trinh order by nam_bat_dau, hoc_ky) as hoc_ky_truoc,
        lag(ty_le_qua_hoc_phan) over (partition by ma_chuong_trinh order by nam_bat_dau, hoc_ky) as ty_le_qua_hoc_phan_truoc,
        lag(gpa_trung_binh) over (partition by ma_chuong_trinh order by nam_bat_dau, hoc_ky) as gpa_trung_binh_truoc,
        lag(ty_le_dung_tien_do) over (partition by ma_chuong_trinh order by nam_bat_dau, hoc_ky) as ty_le_dung_tien_do_truoc
    from metrics
)
select m.*, p.nam_hoc_truoc, p.hoc_ky_truoc,
    p.ty_le_qua_hoc_phan_truoc, p.gpa_trung_binh_truoc, p.ty_le_dung_tien_do_truoc
from metrics m
join narrow_periods p
  on m.ma_chuong_trinh = p.ma_chuong_trinh
 and m.nam_bat_dau = p.nam_bat_dau
 and m.hoc_ky = p.hoc_ky
