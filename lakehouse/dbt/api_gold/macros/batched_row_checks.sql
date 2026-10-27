{% macro batched_row_checks(relation, checks, require_rows=false) %}
with metrics as (
    select count(*) as total_rows
    {% for name, predicate in checks %}
        , count_if({{ predicate }}) as metric_{{ loop.index }}
    {% endfor %}
    from {{ relation }}
)
select check_name, failure_count
from metrics
cross join unnest(array[
{% for name, predicate in checks %}
    cast(row('{{ name }}', coalesce(metric_{{ loop.index }}, 0)) as row(check_name varchar, failure_count bigint)){% if not loop.last or require_rows %},{% endif %}
{% endfor %}
{% if require_rows %}
    cast(row('learning_period_nonempty', if(total_rows = 0, bigint '1', bigint '0')) as row(check_name varchar, failure_count bigint))
{% endif %}
]) as checks(check_name, failure_count)
where failure_count > 0
{% endmacro %}
