select ma_lop_hoc_phan, nam_hoc, hoc_ky
from {{ ref('fct_teaching_progress') }}
group by ma_lop_hoc_phan, nam_hoc, hoc_ky
having count(*) > 1
