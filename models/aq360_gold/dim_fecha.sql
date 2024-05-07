with max_fecha as (
    select max(DATEFROMPARTS(Fech_mes / 100, 12, 31)) fechamax from {{ ref("slv_fact_facturacion") }}
)
select df.* 
from {{ source('aq360_silver', 'slv_dim_fecha') }} df
inner join max_fecha
ON df.Fecha_date <= max_fecha.fechamax
