{% snapshot dim_rel_empresa_explotacion %}

{{
   config(
       target_schema='aq360_silver',
       unique_key='concat(sk_empresa, sk_explotacion)',
       strategy='check',
       check_cols='all',
       invalidate_hard_deletes=True,
       post_hook=[" {{ gen_int_surrogate_keys(this, 'dbt_scd_id', 'id')}} "]
   )
}}

SELECT 
cast(null as int) as id,
sdes.*
from {{ ref("slv_dim_rel_empresa_explotacion") }} sdes

{% endsnapshot %}