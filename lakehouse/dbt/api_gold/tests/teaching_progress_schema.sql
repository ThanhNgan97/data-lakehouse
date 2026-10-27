{{ assert_output_schema(ref('fct_teaching_progress'), [
    ('ma_don_vi', 'varchar'),
    ('ten_don_vi', 'varchar'),
    ('ma_lop_hoc_phan', 'varchar'),
    ('nam_hoc', 'varchar'),
    ('hoc_ky', 'integer'),
    ('ty_le_tien_do_giang_day', 'double'),
    ('so_ban_ghi_theo_doi', 'bigint'),
    ('source_record_id', 'varchar'),
    ('source_updated_at', 'timestamp(6) with time zone'),
    ('source_batch_id', 'varchar'),
    ('source_record_checksum', 'varchar'),
    ('_gold_updated_at', 'timestamp(6) with time zone')
]) }}
