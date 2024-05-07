{% snapshot dim_estructura_geografica_hist %}

{{
   config(
       target_schema='aq360_silver',
       unique_key='sk_poblacion',
       strategy='check',
       check_cols=["des_pais","des_ccaa","des_provincia","des_municipio","des_pedania","des_poblacion"],
       invalidate_hard_deletes=True,
       post_hook=[" {{ gen_int_surrogate_keys(this, 'dbt_scd_id', 'id')}} "]
   )
}}
SELECT
cast(null as int) as id,
sk_poblacion,
des_pais,
des_ccaa,
des_provincia,
des_municipio,
des_pedania,
des_poblacion
from {{ ref("slv_dim_estructura_geografica") }}

{% endsnapshot %}