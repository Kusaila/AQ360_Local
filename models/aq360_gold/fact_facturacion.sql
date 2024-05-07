-- depends_on: {{ ref('cnt_fecha_incremental') }}
{{
  config({
    "materialized" : "incremental",
    "unique_key" : "Fech_mes",
    "as_columnstore" : 'false',
    "pre-hook" : ["{{ drop_all_indexes_on_table() }}"],
    "post-hook" : [
        "{{ create_nonclustered_index(columns=['id_perimetro','id_tipo_calculo', 'id_alta_baja','sk_empresa', 'sk_explotacion', 'sk_poblacion'], 
        includes=['dias_lectura', 'importe_facturado_canon', 'importe_facturado_sincanon','importe_facturado_total', 'vol_fact']) }}"
    ]
    })
}}

with fecha as (
    select Fech_mes,
        Fecha_date,
        Fecha_int,
        RANK() OVER (
            PARTITION BY Fech_mes
            ORDER BY Fecha_date
        ) ranking
    from {{ ref('dim_fecha') }} df
), 
factura as (
    select deg.sk_poblacion,
        deg.cd_poblacion,
        deo.sk_explotacion,
        deo.cd_explotacion,
        des.sk_empresa,
        des.cd_empresa,
        stf.id_perimetro,
        stf.id_canon,
        stf.id_tipo_calculo,
        stf.id_alta_baja,
        fecha.Fecha_int,
        fecha.Fecha_date,
        stf.Fech_mes,
        stf.Fech_Fact,
        stf.Peri_fact,
        stf.m3_fact,
        stf.dias_x_m3,
        stf.Importe
    from {{ ref("slv_fact_facturacion") }} stf
        inner join {{ ref("dim_estructura_geografica") }} deg on stf.sk_poblacion = deg.cd_poblacion
        and (
            stf.Fech_mes > deg.Fech_mes_ini
            and stf.Fech_mes < deg.Fech_mes_fin
        )
        inner join {{ ref("dim_estructura_organizativa") }} deo on stf.sk_explotacion = deo.cd_explotacion
        and (
            stf.Fech_mes >= deo.Fech_mes_ini
            and stf.Fech_mes < deo.Fech_mes_fin
        )
        inner join {{ ref("dim_estructura_societaria") }} des on stf.sk_empresa = des.cd_empresa
        and (
            stf.Fech_mes >= des.Fech_mes_ini
            and stf.Fech_mes < des.Fech_mes_fin
        )
        inner join fecha on stf.Fech_mes = fecha.Fech_mes
    where fecha.ranking = 1 
    {% if is_incremental() %}
        and stf.Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla in ( 'stg_tFacturas2','stg_tFacServConcep') )
    {% endif %}
union all
select deg.sk_poblacion,
        deg.cd_poblacion,
        deo.sk_explotacion,
        deo.cd_explotacion,
        des.sk_empresa,
        des.cd_empresa,
        sfm.id_perimetro,
        sfm.id_canon,
        sfm.id_tipo_calculo,
        sfm.id_alta_baja,
        fecha.Fecha_int,
        fecha.Fecha_date,        
        sfm.Fech_mes,
        sfm.Fech_Fact,
        sfm.Peri_fact,
        sfm.m3_fact,
        sfm.dias_x_m3,
        sfm.Importe
    from {{ ref('slv_facturas_manual') }} sfm
        inner join {{ ref("dim_estructura_geografica") }}  deg on sfm.cd_poblacion = deg.cd_poblacion
        and (
            sfm.Fech_mes > deg.Fech_mes_ini
            and sfm.Fech_mes < deg.Fech_mes_fin
        )
        inner join {{ ref("dim_estructura_organizativa") }} deo on sfm.cd_explotacion = deo.cd_explotacion
        and (
            sfm.Fech_mes >= deo.Fech_mes_ini
            and sfm.Fech_mes < deo.Fech_mes_fin
        )
        inner join {{ ref("dim_estructura_societaria") }} des on sfm.cd_empresa = des.cd_empresa
        and (
            sfm.Fech_mes >= des.Fech_mes_ini
            and sfm.Fech_mes < des.Fech_mes_fin
        )
        inner join fecha on sfm.Fech_mes = fecha.Fech_mes
    where fecha.ranking = 1 
    {% if is_incremental() %}
        and stf.Fech_mes in (select Fech_mes from {{ ref('cnt_fecha_incremental') }}  where tabla in ( 'stg_tFacturas2','stg_tFacServConcep') )
    {% endif %}    
),
intermedia as (
    SELECT Peri_fact periodo,
        fech_mes,
        Fecha_int,
        sk_explotacion,
        cd_explotacion,
        sk_poblacion,
        cd_poblacion,
        sk_empresa,
        cd_empresa,
        id_perimetro,
        id_tipo_calculo,
        id_alta_baja,
        Fech_Fact,
        max(m3_fact) vol_fact,
        max(dias_x_m3) dias_x_m3,
        sum(
            case
                when id_canon = 1 then Importe
                else 0
            end
        ) importe_facturado_canon,
        sum(
            case
                when id_canon = 0 then Importe
                else 0
            end
        ) importe_facturado_sincanon
    FROM factura
    group by Peri_fact,
        fech_mes,
        Fecha_int,
        sk_explotacion,
        cd_explotacion,
        sk_poblacion,
        cd_poblacion,
        sk_empresa,
        cd_empresa,
        id_perimetro,
        id_tipo_calculo,
        id_alta_baja,
        Fech_Fact
)
select intermedia.periodo,
    intermedia.fech_mes,
    intermedia.Fecha_int fecha,
    intermedia.sk_explotacion,
    intermedia.cd_explotacion,
    intermedia.sk_poblacion,
    intermedia.cd_poblacion,
    intermedia.sk_empresa,
    intermedia.cd_empresa,
    intermedia.id_perimetro,
    intermedia.id_tipo_calculo,
    intermedia.id_alta_baja,
    intermedia.Fech_Fact,
    intermedia.vol_fact,
    case
        when intermedia.vol_fact = 0 then 0
        else intermedia.dias_x_m3 / intermedia.vol_fact
    end dias_lectura,
    intermedia.dias_x_m3,
    intermedia.importe_facturado_canon,
    intermedia.importe_facturado_sincanon,
    intermedia.importe_facturado_canon + intermedia.importe_facturado_sincanon importe_facturado_total
from intermedia