{{
  config(
    materialized = 'table',
    test = 'unique'
    )
}}
select 
    CdPeriodo,
    CdTipPeriodo,
    DsTipPeriodo,
    Anno,
    NumDias,
    Periodo,
    CAST(CAST(Periodo as int)-1000 as varchar(7)) Periodo_ant,
    sAnno
from {{ source('aq360_bronze', 'stg_tPeriodo') }}