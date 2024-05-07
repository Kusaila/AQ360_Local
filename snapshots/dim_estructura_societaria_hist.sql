{% snapshot dim_estructura_societaria_hist %}

{{
   config(
       target_schema='aq360_silver',
       unique_key='sk_empresa',
       strategy='check',
       check_cols=["cif","TipoEmpresa","nombre_empresa"],
       invalidate_hard_deletes=True,
       post_hook=[" {{ gen_int_surrogate_keys(this, 'dbt_scd_id', 'id')}} "]
   )
}}
SELECT
cast(null as int) as id,
sk_empresa,
cif,
TipoEmpresa,
nombre_empresa
from {{ ref("slv_dim_estructura_societaria") }}

{% endsnapshot %}