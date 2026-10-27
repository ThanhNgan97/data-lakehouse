{{ batched_row_checks(ref('fct_learning_outcomes'), [
    ("not_null_fct_learning_outcomes_ma_chuong_trinh", "ma_chuong_trinh is null"),
    ("not_null_fct_learning_outcomes_nam_hoc", "nam_hoc is null"),
    ("not_null_fct_learning_outcomes_nam_bat_dau", "nam_bat_dau is null"),
    ("not_null_fct_learning_outcomes_hoc_ky", "hoc_ky is null"),
    ("not_null_fct_learning_outcomes_source_record_id", "source_record_id is null"),
    ("not_null_fct_learning_outcomes_source_updated_at", "source_updated_at is null"),
    ("not_null_fct_learning_outcomes_source_batch_id", "source_batch_id is null"),
    ("not_null_fct_learning_outcomes_source_record_checksum", "source_record_checksum is null"),
    ("not_null_fct_learning_outcomes__gold_updated_at", "_gold_updated_at is null"),
    ("learning_outcomes_ranges", "(ty_le_qua_hoc_phan is not null and (is_nan(ty_le_qua_hoc_phan) or is_infinite(ty_le_qua_hoc_phan) or ty_le_qua_hoc_phan<0 or ty_le_qua_hoc_phan>100))\n   or (gpa_trung_binh is not null and (is_nan(gpa_trung_binh) or is_infinite(gpa_trung_binh) or gpa_trung_binh<0))\n   or (ty_le_dung_tien_do is not null and (is_nan(ty_le_dung_tien_do) or is_infinite(ty_le_dung_tien_do) or ty_le_dung_tien_do<0 or ty_le_dung_tien_do>100))"),
    ("learning_outcomes_formulas", "ty_le_qua_hoc_phan is distinct from (case when tong_luot_hoc_phan>0 then cast(so_luot_hoc_phan_dat as double) * 100.0 / cast(tong_luot_hoc_phan as double) end)\n   or gpa_trung_binh is distinct from (case when so_sinh_vien_tinh_gpa>0 then cast(tong_diem_gpa as double) * 1.0 / cast(so_sinh_vien_tinh_gpa as double) end)\n   or ty_le_dung_tien_do is distinct from (case when so_sinh_vien_danh_gia_tien_do>0 then cast(so_sinh_vien_dung_tien_do as double) * 100.0 / cast(so_sinh_vien_danh_gia_tien_do as double) end)"),
], require_rows=false) }}
