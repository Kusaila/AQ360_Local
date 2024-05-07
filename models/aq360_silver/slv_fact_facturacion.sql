-- depends_on: {{ ref('cnt_fecha_incremental') }}
{{
  config(
    materialized = 'incremental',
    unique_key = 'Fech_mes',
    )
}}

with factura as (
select  *, 1.00*stf.Num_Dias_Lect*stf.M3_fact as dias_x_m3
from {{ ref('slv_fact_facturacion_ini') }} stf 
{% if is_incremental() %}

where stf.Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla in ( 'stg_tFacturas2','stg_tFacServConcep') )

{% endif %}
),
empresas_prop as (
    select distinct sdtme.ID_COMPANY,
        sdtme.Proporcional,
        sdtme.Consolidado
    from {{ source('aq360_bronze', 'stg_dw_t_mEmpresas') }} sdtme
        inner join {{ source('aq360_bronze', 'stg_dw_t_mEmpresas_Evol') }} sdtmee
        on sdtme.CdDelegacionCont = sdtme.CdDelegacionCont
    where sdtme.Proporcional != 0
    and sdtmee.dFech_Fin is null
),
empresas_cons as (
    select distinct sdtme.ID_COMPANY,
        sdtme.Proporcional,
        sdtme.Consolidado
    from {{ source('aq360_bronze', 'stg_dw_t_mEmpresas') }} sdtme
        inner join {{ source('aq360_bronze', 'stg_dw_t_mEmpresas_Evol') }} sdtmee
        on sdtme.CdDelegacionCont = sdtme.CdDelegacionCont
    where sdtme.Consolidado != 0
    and sdtmee.dFech_Fin is null
)    
select 
    stf.sk_poblacion,
    stf.sk_explotacion,
    stf.sk_empresa,
    stf.id_perimetro,
    stf.id_canon, 
    3 id_tipo_calculo,
    stf.id_alta_baja,
    stf.Fech_mes,
    stf.Fech_Fact,
    stf.Peri_fact,
    stf.m3_fact,
    stf.dias_x_m3,
    isnull(stf.Importe,0)*(1.00*empresas_prop.Proporcional/100) Importe
from factura stf
inner join empresas_prop on stf.sk_empresa = empresas_prop.id_company
{% if is_incremental() %}
where stf.Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla in ( 'stg_tFacturas2','stg_tFacServConcep') )
{% endif %}
union
select 
    stf.sk_poblacion,
    stf.sk_explotacion,
    stf.sk_empresa,
    stf.id_perimetro,
    stf.id_canon, 
    2 id_tipo_calculo,
    stf.id_alta_baja,
    stf.Fech_mes,
    stf.Fech_Fact,    
    stf.Peri_fact,
    stf.m3_fact,
    stf.dias_x_m3,
    isnull(stf.Importe,0)*(1.00*empresas_cons.Consolidado/100) Importe
from factura stf
inner join empresas_cons on stf.sk_empresa = empresas_cons.id_company
{% if is_incremental() %}
where stf.Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla in ( 'stg_tFacturas2','stg_tFacServConcep') )
{% endif %}
union
select 
    stf.sk_poblacion,
    stf.sk_explotacion,
    stf.sk_empresa,
    stf.id_perimetro,
    stf.id_canon, 
    1 id_tipo_calculo,
    stf.id_alta_baja,
    stf.Fech_mes,
    stf.Fech_Fact,    
    stf.Peri_fact,
    stf.m3_fact,
    stf.dias_x_m3,
    isnull(stf.Importe,0)
from factura stf
{% if is_incremental() %}
where stf.Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla in ( 'stg_tFacturas2','stg_tFacServConcep') )
{% endif %}