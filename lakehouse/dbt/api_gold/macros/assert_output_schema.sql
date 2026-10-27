{% macro assert_output_schema(relation, columns) %}
with expected(column_name, data_type, ordinal_position) as (
    values
    {% for name, type in columns %}
    ('{{ name }}', '{{ type }}', {{ loop.index }}){% if not loop.last %},{% endif %}
    {% endfor %}
), actual as (
    select column_name, data_type, ordinal_position
    from {{ adapter.quote(relation.database) }}.information_schema.columns
    where table_schema = '{{ relation.schema }}' and table_name = '{{ relation.identifier }}'
)
select coalesce(e.column_name, a.column_name) as column_name
from expected e full outer join actual a on e.ordinal_position = a.ordinal_position
where e.column_name is distinct from a.column_name or e.data_type is distinct from a.data_type
{% endmacro %}
