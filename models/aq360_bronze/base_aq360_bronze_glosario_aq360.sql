with source as (
      select * from {{ source('aq360_bronze', 'glosario_aq360') }}
),
renamed as (
    select
        {{ adapter.quote("Agrupación/Pantalla") }},
        {{ adapter.quote("KPI/Nombre/Argumento") }},
        {{ adapter.quote("Unidad medida") }},
        {{ adapter.quote("Descripción") }},
        {{ adapter.quote("Cálculo") }}

    from source
)
select * from renamed
  