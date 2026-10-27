select ma_chuong_trinh, nam_hoc, hoc_ky
from {{ ref('fct_learning_outcomes') }}
group by 1,2,3 having count(*) > 1
