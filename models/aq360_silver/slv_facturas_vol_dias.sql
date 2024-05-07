-- depends_on: {{ ref('cnt_fecha_incremental') }}
{{
  config(
    materialized = 'incremental',
    unique_key = 'Fech_mes',
    )
}}

with stg_tFacturas2 as (
      select * from {{ source('aq360_bronze', 'stg_tFacturas2') }}

     {% if is_incremental() %}

      where Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla = 'stg_tFacturas2' )

    {% endif %}
),
final as (
    select
    cast (dtej.sk_empresa as int) sk_empresa,
    cast(tf.CdPoblacion as bigint) CdPoblacion,
    cast(tf.CdExplotacion as int) CdExplotacion,
    0 id_perimetro,
    dtc.id_alta_baja,
    tf.Fech_mes,
    tf.Peri_fact,
    tf.Num_Dias_Lect,
    tf.Fech_Fact
    from stg_tFacturas2 tf
    inner join {{ ref("slv_dim_eqJuridica") }} dtej
    on dtej.nCod_inst = tf.ncod_inst 
    and dtej.nCod_expl = tf.ncod_expl 
    and dtej.nCod_pobl  = tf.ncod_pobl 
    and dtej.nCod_emi = tf.ncod_emi
    inner join {{ ref("slv_dim_tipocliente") }} dtc
    on tf.CdTipCli = dtc.CdTipCli
    where dtc.fec_fin_vig is null
    and dtej.dFech_ini_del < DATEADD(DAY, -1, DATEADD(MONTH, 1, CAST(CAST(tf.Fech_mes AS VARCHAR(6)) + '01' AS DATE)))
    and (dtej.dFech_fin_del > DATEADD(DAY, -1, DATEADD(MONTH, 1, CAST(CAST(tf.Fech_mes AS VARCHAR(6)) + '01' AS DATE))) or dtej.dFech_fin_del is null)
)
select * from final
