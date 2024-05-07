{% macro create_nonclustered_index(columns, includes=False) %}

{{ log("Creating nonclustered index...") }}

{% if includes -%}
    {% set idx_name = (
        "nonclustered_"
        + local_md5(columns | join("_"))
        + "_incl_"
        + local_md5(includes | join("_"))
    ) %}
{% else -%}
    {% set idx_name = "nonclustered_" + local_md5(columns | join("_")) %}
{% endif %}

if not exists(select *
                from sys.indexes {{ information_schema_hints() }}
                where name = '{{ idx_name }}'
                and object_id = OBJECT_ID('{{ this }}')
)
begin
create nonclustered index
    {{ idx_name }}
      on {{ this }} ({{ '[' + columns|join("], [") + ']' }})
      {% if includes -%}
        include ({{ '[' + includes|join("], [") + ']' }})
      {% endif %}
end
{% endmacro %}