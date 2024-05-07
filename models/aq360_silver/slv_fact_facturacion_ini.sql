-- depends_on: {{ ref('cnt_fecha_incremental') }}
{{
  config(
    materialized = 'incremental',
    unique_key = 'Fech_mes',
    )
}}

with factura_vol_dias as (
select    stf.CdPoblacion sk_poblacion,
    stf.CdExplotacion sk_explotacion,
    stf.sk_empresa sk_empresa,
    stf.id_perimetro id_perimetro,
    stf.id_alta_baja id_alta_baja,
    stf.Fech_mes Fech_mes,
    stf.Peri_fact Peri_fact,
    stf.Fech_Fact Fech_Fact,
    stf.Num_Dias_Lect
from {{ ref('slv_facturas_vol_dias') }} stf 
{% if is_incremental() %}
where Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla = 'stg_tFacServConcep' )
{% endif %}
group by stf.CdPoblacion,
    stf.CdExplotacion,
    stf.sk_empresa,
    stf.id_perimetro,
    stf.id_alta_baja,
    stf.Fech_mes,
    stf.Fech_Fact,
    stf.Peri_fact,
    stf.Num_Dias_Lect
),facturas_importes_sin_canon as (
select    
    stf.CdPoblacion sk_poblacion,
    stf.CdExplotacion sk_explotacion,
    stf.sk_empresa sk_empresa,
    stf.flag_perimetro id_perimetro,
    stf.id_alta_baja id_alta_baja,
    stf.id_canon id_canon,
    stf.Fech_mes Fech_mes,
    stf.Fech_Fact,
    stf.ind_peri Peri_fact,
    stf.m3_fact,
    stf.Importe Importe
from {{ ref('slv_facturas_importes') }} stf 
where stf.id_canon = 0
{% if is_incremental() %}
and Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla = 'stg_tFacServConcep' )
{% endif %}
),facturas_importes_canon as (
select    
    stf.CdPoblacion sk_poblacion,
    stf.CdExplotacion sk_explotacion,
    stf.sk_empresa sk_empresa,
    stf.flag_perimetro id_perimetro,
    stf.id_alta_baja id_alta_baja,
    stf.id_canon id_canon,
    stf.Fech_mes Fech_mes,
    stf.Fech_Fact Fech_Fact,
    stf.ind_peri Peri_fact,
    stf.m3_fact,
    stf.Importe Importe_canon
from {{ ref('slv_facturas_importes') }} stf 
where stf.id_canon = 1
{% if is_incremental() %}
and stf.Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla in ( 'stg_tFacturas2','stg_tFacServConcep') )
{% endif %}
)
select 
    facturas_importes_canon.sk_poblacion sk_poblacion,
    facturas_importes_canon.sk_explotacion sk_explotacion,
    facturas_importes_canon.sk_empresa sk_empresa,
    facturas_importes_canon.id_perimetro id_perimetro,
    facturas_importes_canon.id_alta_baja id_alta_baja,
    1 id_canon,
    facturas_importes_canon.Fech_mes Fech_mes,
    facturas_importes_canon.Fech_Fact Fech_Fact,
    facturas_importes_canon.Peri_fact Peri_fact,
    isnull(factura_vol_dias.Num_Dias_Lect,0) Num_Dias_Lect,
    facturas_importes_canon.m3_fact m3_fact,
    isnull(facturas_importes_canon.Importe_canon,0) Importe
from facturas_importes_canon
left outer join factura_vol_dias
on factura_vol_dias.id_alta_baja = facturas_importes_canon.id_alta_baja
and factura_vol_dias.sk_explotacion = facturas_importes_canon.sk_explotacion
and factura_vol_dias.sk_poblacion = facturas_importes_canon.sk_poblacion
and factura_vol_dias.sk_empresa = facturas_importes_canon.sk_empresa
and factura_vol_dias.Fech_mes = facturas_importes_canon.Fech_mes
and factura_vol_dias.Fech_Fact = facturas_importes_canon.Fech_Fact
and factura_vol_dias.Peri_fact = facturas_importes_canon.Peri_fact
union all
select 
    facturas_importes_sin_canon.sk_poblacion sk_poblacion,
    facturas_importes_sin_canon.sk_explotacion sk_explotacion,
    facturas_importes_sin_canon.sk_empresa sk_empresa,
    facturas_importes_sin_canon.id_perimetro id_perimetro,
    facturas_importes_sin_canon.id_alta_baja id_alta_baja,
    0 id_canon,
    facturas_importes_sin_canon.Fech_mes Fech_mes,
    facturas_importes_sin_canon.Fech_Fact Fech_Fact,    
    facturas_importes_sin_canon.Peri_fact Peri_fact,
    isnull(factura_vol_dias.Num_Dias_Lect,0) Num_Dias_Lect,  
    facturas_importes_sin_canon.m3_fact m3_fact,
    isnull(facturas_importes_sin_canon.Importe,0) Importe
from factura_vol_dias
left outer join facturas_importes_sin_canon
on factura_vol_dias.id_alta_baja = facturas_importes_sin_canon.id_alta_baja
and factura_vol_dias.sk_explotacion = facturas_importes_sin_canon.sk_explotacion
and factura_vol_dias.sk_poblacion = facturas_importes_sin_canon.sk_poblacion
and factura_vol_dias.sk_empresa = facturas_importes_sin_canon.sk_empresa
and factura_vol_dias.Fech_mes = facturas_importes_sin_canon.Fech_mes
and factura_vol_dias.Fech_Fact = facturas_importes_sin_canon.Fech_Fact
and factura_vol_dias.Peri_fact = facturas_importes_sin_canon.Peri_fact