{% macro log_sql(statement) %}
  {{ log(statement, info=True) }}
  {{ return(statement) }}
{% endmacro %}
