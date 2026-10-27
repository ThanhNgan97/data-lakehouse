{{ assert_output_schema(ref('agg_learning_outcomes_by_period'), [
    ('nam_hoc', 'varchar'),
    ('hoc_ky', 'integer'),
    ('so_chuong_trinh', 'bigint'),
    ('so_luot_hoc_phan_dat', 'bigint'),
    ('tong_luot_hoc_phan', 'bigint'),
    ('ty_le_qua_hoc_phan', 'double'),
    ('tong_diem_gpa', 'double'),
    ('so_sinh_vien_tinh_gpa', 'bigint'),
    ('gpa_trung_binh', 'double'),
    ('so_sinh_vien_dung_tien_do', 'bigint'),
    ('so_sinh_vien_danh_gia_tien_do', 'bigint'),
    ('ty_le_dung_tien_do', 'double'),
    ('_gold_updated_at', 'timestamp(6) with time zone')
]) }}
