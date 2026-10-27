select nam_hoc, hoc_ky from {{ ref('agg_learning_outcomes_by_period') }}
group by 1,2 having count(*)>1
