-- depends_on: {{ ref('cnt_fecha_incremental') }}
{{
  config(
    materialized = 'incremental',
    unique_key = 'Fech_mes',
    )
}}

with slv_facturas_manual_inicial as (
    select 
        cd_explotacion,
        cd_empresa,
        cd_poblacion,
        Fech_mes,
        Fech_Fact,
        CdPeriodo,
        id_canon,
        id_alta_baja,
        flag_perimetro,
        vol_fact,
        1.00*dias_lectura*vol_fact dias_x_m3,
        Importe
    from {{ ref('slv_facturas_manual_inicial') }} 

    {% if is_incremental() %}

      where  PERIODO_ANALISIS in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla = 'stg_tExploMan_FactServConc_GrupSeg' )

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
    stf.cd_explotacion,
    stf.cd_poblacion,
    stf.cd_empresa,
    stf.flag_perimetro id_perimetro,
    stf.id_canon, 
    3 id_tipo_calculo,
    stf.id_alta_baja,
    stf.Fech_mes,
    stf.Fech_Fact,
    stf.CdPeriodo Peri_fact,
    vol_fact m3_fact,
    dias_x_m3,
    isnull(stf.Importe,0)*(1.00*empresas_prop.Proporcional/100) Importe
from slv_facturas_manual_inicial stf
inner join empresas_prop on stf.cd_empresa = empresas_prop.id_company
{% if is_incremental() %}
where stf.Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla in ( 'stg_tFacturas2','stg_tFacServConcep') )
{% endif %}
union
select 
    stf.cd_explotacion,
    stf.cd_poblacion,
    stf.cd_empresa,
    stf.flag_perimetro id_perimetro,
    stf.id_canon, 
    2 id_tipo_calculo,
    stf.id_alta_baja,
    stf.Fech_mes,
    stf.Fech_Fact,    
    stf.CdPeriodo Peri_fact,
    vol_fact m3_fact,
    dias_x_m3,
    isnull(stf.Importe,0)*(1.00*empresas_cons.Consolidado/100) Importe
from slv_facturas_manual_inicial stf
inner join empresas_cons on stf.cd_empresa = empresas_cons.id_company
{% if is_incremental() %}
where stf.Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla in ( 'stg_tFacturas2','stg_tFacServConcep') )
{% endif %}
union
select 
    stf.cd_explotacion,
    stf.cd_poblacion,
    stf.cd_empresa,
    stf.flag_perimetro id_perimetro,
    stf.id_canon, 
    1 id_tipo_calculo,
    stf.id_alta_baja,
    stf.Fech_mes,
    stf.Fech_Fact,    
    stf.CdPeriodo Peri_fact,
    vol_fact m3_fact,
    dias_x_m3,
    isnull(stf.Importe,0)
from slv_facturas_manual_inicial stf
{% if is_incremental() %}
where stf.Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla in ( 'stg_tFacturas2','stg_tFacServConcep') )
{% endif %}