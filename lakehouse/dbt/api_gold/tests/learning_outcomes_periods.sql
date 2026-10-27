with expected as (
select *,
    lag(nam_hoc) over (partition by ma_chuong_trinh order by nam_bat_dau, hoc_ky) as expected_nam_hoc,
    lag(hoc_ky) over (partition by ma_chuong_trinh order by nam_bat_dau, hoc_ky) as expected_hoc_ky,
    lag(ty_le_qua_hoc_phan) over (partition by ma_chuong_trinh order by nam_bat_dau, hoc_ky) as expected_ty_le_qua_hoc_phan,
    lag(gpa_trung_binh) over (partition by ma_chuong_trinh order by nam_bat_dau, hoc_ky) as expected_gpa_trung_binh,
    lag(ty_le_dung_tien_do) over (partition by ma_chuong_trinh order by nam_bat_dau, hoc_ky) as expected_ty_le_dung_tien_do
from {{ ref('fct_learning_outcomes') }}
) select * from expected
where nam_hoc_truoc is distinct from expected_nam_hoc
   or hoc_ky_truoc is distinct from expected_hoc_ky
   or ty_le_qua_hoc_phan_truoc is distinct from expected_ty_le_qua_hoc_phan
   or gpa_trung_binh_truoc is distinct from expected_gpa_trung_binh
   or ty_le_dung_tien_do_truoc is distinct from expected_ty_le_dung_tien_do
   or chenh_lech_ty_le_qua_hoc_phan_pp is distinct from (ty_le_qua_hoc_phan - ty_le_qua_hoc_phan_truoc)
   or chenh_lech_gpa_trung_binh is distinct from (gpa_trung_binh - gpa_trung_binh_truoc)
   or chenh_lech_ty_le_dung_tien_do_pp is distinct from (ty_le_dung_tien_do - ty_le_dung_tien_do_truoc)
