--Valida la existencia de la tabla, y de no ser asi la crea pasando una lista de diccionarios con los campos a crear
{% macro validar_y_crear_tabla(nombre_tabla, campos) %}

    {% set tabla_existe = adapter.get_relation(database='sqld-aq-360-360-pre-01',
                                schema='aq360_bronze',
                                identifier='test') is not none -%}

    {% if not table_exists %}
        -- Si la tabla no existe, la creamos
        CREATE TABLE {{ nombre_tabla }} (
            {% for campo in campos %}
                {{ campo.nombre }} {{ campo.tipo }} {% if campo.obligatorio %}NOT NULL{% endif %}{% if not loop.last %},{% endif %}
            {% endfor %}
            -- Añadir aquí las claves primarias, foráneas, etc. si es necesario
        );

    {% endif %}

{% endmacro %}
