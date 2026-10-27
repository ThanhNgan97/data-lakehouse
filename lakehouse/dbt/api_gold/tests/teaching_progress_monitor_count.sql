select * from {{ ref('fct_teaching_progress') }}
where so_ban_ghi_theo_doi is null or so_ban_ghi_theo_doi <> 1
