{% snapshot dim_estructura_organizativa_hist %}

{{
   config(
       target_schema='aq360_silver',
       unique_key='sk_explotacion',
       strategy='check',
       check_cols=["des_ambito","des_zona","des_delegacion","des_ugestion","des_contrata","Tipo_Org"],
       invalidate_hard_deletes=True,
       post_hook=[" {{ gen_int_surrogate_keys(this, 'dbt_scd_id', 'id')}} "]
   )
}}

SELECT
cast(null as int) as id,
sk_explotacion,
des_ambito,
des_zona,
des_delegacion,
des_ugestion,
des_contrata,
des_explotacion,
Tipo_Org
from {{ ref("slv_dim_estructura_organizativa_hist") }}

{% endsnapshot %}