{{
  config(
    pre_hook=[
    "DELETE aq360_web.dim_rel_empresa_explotacion;" 
  ],
  materialized='incremental'
    )
}}

select * from {{ source('aq360_silver', 'dim_rel_empresa_explotacion_fin') }}