    select sk_poblacion,
    des_pais,
    des_ccaa,
    des_provincia,
    des_municipio,
    des_pedania,
    des_poblacion
     from {{ ref('slv_dim_estructura_geografica') }}

