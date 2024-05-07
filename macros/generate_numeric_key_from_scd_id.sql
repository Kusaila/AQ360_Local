{% macro generate_numeric_key_from_scd_id(snapshot_name, schema_name) %}
  {% set full_snapshot_name = schema_name ~ '.' ~ snapshot_name %}
  {% for row in run_query('select dbt_scd_id from ' ~ full_snapshot_name) %}
    -- Convierte el hash MD5 (dbt_scd_id) en un número entero único
    {{ return('abs(convert(bigint, hashbytes(''md5'', ' ~ row.dbt_scd_id ~ ')))') }}
  {% endfor %}
{% endmacro %}