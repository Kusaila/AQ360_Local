-- depends_on: {{ ref('cnt_fecha_incremental') }}
{{
  config(
    materialized = 'incremental',
    unique_key = 'Fech_mes',
    )
}}

with stg_tFacServConcep as (
    select *
    from {{ source('aq360_bronze', 'stg_tFacServConcep') }}

    {% if is_incremental() %}

      where Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla = 'stg_tFacServConcep' )

    {% endif %}
),
fac_canon as (
    select tf.CdPoblacion,
        tf.CdExplotacion,
        dtej.sk_empresa,
        0 flag_perimetro,
        dtc.id_alta_baja,
        case
            when charindex(UPPER('Canon'), UPPER(dcs.des_ser)) != 0 then 1
            else 0
        end id_canon,
        tf.Fech_mes,
        tf.Fech_Fact,
        tf.ind_peri,
        tf.imp_bas_fac_emi,
        tf.m3_fact_emi
    from stg_tFacServConcep tf
        inner join {{ ref("slv_dim_eqJuridica") }} dtej on dtej.nCod_inst = tf.ncod_inst
        and dtej.nCod_expl = tf.ncod_expl
        and dtej.nCod_pobl = tf.ncod_pobl
        and dtej.nCod_emi = tf.ncod_emi
        inner join {{ ref("slv_dim_tipocliente") }} dtc on tf.cod_tip_cli = dtc.CdTipCli
        left outer join {{ ref("slv_dim_concepto_servicio") }} dcs on tf.cod_ser_concepto = dcs.cod_ser
        and tf.cod_concepto = dcs.cod_concep
        and tf.ncod_inst = dcs.cod_inst
        and tf.cod_ctta_concepto = dcs.cod_ctta
    where dtc.fec_fin_vig is null 
    and dtej.dFech_ini_del < DATEADD(DAY, -1, DATEADD(MONTH, 1, CAST(CAST(tf.Fech_mes AS VARCHAR(6)) + '01' AS DATE)))
and (dtej.dFech_fin_del > DATEADD(DAY, -1, DATEADD(MONTH, 1, CAST(CAST(tf.Fech_mes AS VARCHAR(6)) + '01' AS DATE))) or dtej.dFech_fin_del is null)
)
select CdPoblacion,
    CdExplotacion,
    sk_empresa,
    flag_perimetro,
    id_alta_baja,
    id_canon,
    Fech_mes,
    ind_peri,
    Fech_Fact,
    sum(m3_fact_emi) m3_fact,
    sum(imp_bas_fac_emi) Importe
from fac_canon
group by CdPoblacion,
    CdExplotacion,
    sk_empresa,
    flag_perimetro,
    id_alta_baja,
    id_canon,
    Fech_mes,
    Fech_Fact,
    ind_peri