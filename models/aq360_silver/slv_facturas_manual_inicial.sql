-- depends_on: {{ ref('cnt_fecha_incremental') }}
{{
  config(
    materialized = 'incremental',
    unique_key = 'Fech_mes',
    )
}}
with stg_tExploMan_FactServConc_GrupSeg as (
    select 
        DATE_INFO,
        cast(ID_COMPANY as int) cd_explotacion,
        cast(ID_COMPANY_EMPRESA as int) cd_empresa,
        PERIODO_ANALISIS Fech_mes,
        case
            when CHARINDEX('Canon', SERVICIO_NORMALIZADO) != 0 then 1
        else 0 end id_canon,
        case
            when COD_GRUPO_SEGMENTO = 10 then 1
            when COD_GRUPO_SEGMENTO = 1 then 2
        else 3 end id_alta_baja,
        M3_FACTURADO vol_fact,
        IMP_BASE_FACTURADO Importe
    from {{ source('aq360_bronze', 'stg_tExploMan_FactServConc_GrupSeg') }}
    where ID_COMPANY is not null
    {% if is_incremental() %}
      and  PERIODO_ANALISIS in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla = 'stg_tExploMan_FactServConc_GrupSeg' )
    {% endif %}
)
select 
    fact.cd_explotacion,
    fact.cd_empresa,
    cast(sregh.CdPoblacion as bigint) cd_poblacion,
    fact.Fech_mes,
    srepe.Fecha_fin_periodo Fech_Fact,
    srepe.CdPeriodo,
    id_canon,
    0 as flag_perimetro,
    id_alta_baja,
    srepe.Dias as dias_lectura,
    sum(fact.vol_fact) vol_fact,
    sum(fact.Importe) Importe
from stg_tExploMan_FactServConc_GrupSeg fact
    inner join {{ ref('slv_rel_exploman_periodo_explotacion') }} srepe on fact.cd_explotacion = srepe.cd_explotacion
    and fact.DATE_INFO between srepe.Fecha_ini_periodo and srepe.Fecha_fin_periodo
    inner join  {{ ref('slv_rel_explo_geo_hist') }} sregh on fact.cd_explotacion = sregh.CdExplotacion
    and (
        fact.DATE_INFO between sregh.FECHA_INI_ACTV and sregh.FECHA_FIN_ACTV
        or sregh.FECHA_FIN_ACTV is null
    )
where fact.DATE_INFO is not null
group by fact.cd_explotacion,fact.cd_empresa,srepe.Dias,
    sregh.CdPoblacion,fact.Fech_mes,srepe.Fecha_fin_periodo,fact.id_canon,
    srepe.CdPeriodo, fact.id_alta_baja
