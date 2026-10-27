with counts as (
    select
        (select count(*) from {{ source('silver', 'teaching_progress') }}) as silver_rows,
        (select count(*) from {{ ref('fct_teaching_progress') }}) as gold_rows
)
select * from counts where silver_rows = 0 or gold_rows <> silver_rows
