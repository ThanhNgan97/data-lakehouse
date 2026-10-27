select * from {{ ref('fct_teaching_progress') }}
where ty_le_tien_do_giang_day is null
   or is_nan(ty_le_tien_do_giang_day)
   or ty_le_tien_do_giang_day < 0
   or ty_le_tien_do_giang_day > 100
