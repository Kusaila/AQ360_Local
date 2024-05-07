{% macro scd2_update(target_schema, target_table, staging_schema, staging_table, business_keys, surrogate_key, attributes) %}

{% set tabla_existe = adapter.get_relation(database='sqld-aq-360-360-pre-01',
                                schema=target_schema,
                                identifier=target_table) is not none -%}

    {% if not table_exists %}

    CREATE TABLE {{ target_schema }}.{{ target_table }} (
    {{ surrogate_key }} INT NOT NULL,
    {% for key in business_keys %}
        {{ key.column_name }} {{ key.data_type }} NOT NULL,
    {% endfor %}
    {% for attr in attributes %}
        {{ attr.column_name }} {{ attr.data_type }}{% if attr.is_nullable == 'NO' %} NOT NULL{% endif %},
    {% endfor %}
    start_date DATETIME,
    end_date DATETIME,
    load_date DATETIME
);
{% endif %}  
/*
WITH latest_records AS (
  SELECT
    {{ this }}.{{ surrogate_key }} AS {{ surrogate_key }},
    {% for key in business_keys %}
      {{ this }}.{{ key }} AS {{ key }},
    {% endfor %}
    ROW_NUMBER() OVER (PARTITION BY 
      {% for key in business_keys %}
        {{ this }}.{{ key }}{% if not loop.last %}, {% endif %}
      {% endfor %} 
      ORDER BY {{ this }}.load_date DESC) AS row_num
  FROM {{ this }}
),

new_records AS (
  SELECT
    {% for key in business_keys %}
      stg.{{ key }} AS {{ key }},
    {% endfor %}
    {% for attr in attributes %}
      stg.{{ attr }} AS {{ attr }},
    {% endfor %}
    GETDATE() AS load_date
  FROM {{ staging_schema }}.{{ staging_table }} AS stg
),

updates AS (
  SELECT
    latest.{{ surrogate_key }},
    {% for attr in attributes %}
      CASE WHEN latest.{{ attr }} <> new.{{ attr }} THEN new.{{ attr }} ELSE latest.{{ attr }} END AS {{ attr }},
    {% endfor %}
    GETDATE() AS end_date
  FROM latest_records latest
  JOIN new_records new ON 
    {% for key in business_keys %}
      latest.{{ key }} = new.{{ key }}{% if not loop.last %} AND {% endif %}
    {% endfor %}
    AND latest.row_num = 1
),

inserts AS (
  SELECT
    ISNULL((SELECT MAX({{ surrogate_key }}) + 1 FROM {{ target_schema }}.{{ this }}), 1) AS {{ surrogate_key }},
    {% for key in business_keys %}
      {{ key }},
    {% endfor %}
    {% for attr in attributes %}
      {{ attr }},
    {% endfor %}
    GETDATE() AS start_date,
    NULL AS end_date,
    GETDATE() AS load_date
  FROM new_records
  WHERE 
    (
      {% for key in business_keys %}
        {{ key }}{% if not loop.last %}, {% endif %}
      {% endfor %}
    ) NOT IN 
    (
      SELECT 
        {% for key in business_keys %}
          {{ key }}{% if not loop.last %}, {% endif %}
        {% endfor %}
      FROM latest_records WHERE row_num = 1
    )
),

first_insert AS (
  SELECT
    ISNULL((SELECT MAX({{ surrogate_key }}) + 1 FROM {{ target_schema }}.{{ this }}), 1) AS {{ surrogate_key }},
    {% for key in business_keys %}
      {{ key }},
    {% endfor %}
    {% for attr in attributes %}
      {{ attr }},
    {% endfor %}
    GETDATE() AS start_date,
    NULL AS end_date,
    GETDATE() AS load_date
  FROM {{ staging_schema }}.{{ staging_table }} AS stg
  WHERE 
    (
      {% for key in business_keys %}
        {{ key }}{% if not loop.last %}, {% endif %}
      {% endfor %}
    ) NOT IN 
    (
      SELECT 
        {% for key in business_keys %}
          {{ key }}{% if not loop.last %}, {% endif %}
        {% endfor %}
      FROM latest_records WHERE row_num = 1
    )
)

final AS (
  SELECT * FROM updates
  UNION ALL
  SELECT * FROM inserts
  UNION ALL
  SELECT * FROM first_insert
)

INSERT INTO {{ this }}
SELECT * FROM final;
*/
{% endmacro %}
