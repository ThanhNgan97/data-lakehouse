with counts as (
select (select count(*) from {{ source('silver', 'learning_outcomes') }} where da_xoa=false) s,
       (select count(*) from {{ ref('fct_learning_outcomes') }}) g
) select * from counts where s=0 or s<>g
