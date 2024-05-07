{{
  config(
    materialized = 'table',
    )
}}
with bvsdteg as (
    SELECT distinct CdPoblacion,ID_GEOENTITY
    from {{ source('aq360_bronze', 'stg_dw_t_eqGeografica') }} 
)
SELECT 
   distinct bvsdteg.CdPoblacion as sk_poblacion,
    pais.[ENTITY_NAME] des_pais,
    CCAA.[ENTITY_NAME] des_ccaa,
    provincia.[ENTITY_NAME] des_provincia,
    municipio.[ENTITY_NAME] des_municipio,
    pedania.[ENTITY_NAME] des_pedania,
    poblacion.[ENTITY_NAME] des_poblacion,
    poblacion.ID_GEO_ENTITY id_geo_entity_poblacion
FROM 
    {{ source('aq360_bronze', 'stg_dim_geographic_entity') }} as mundo,
    {{ source('aq360_bronze', 'stg_dim_geographic_entity') }} as pais,
    {{ source('aq360_bronze', 'stg_dim_geographic_entity') }} as CCAA,
    {{ source('aq360_bronze', 'stg_dim_geographic_entity') }} as provincia,
    {{ source('aq360_bronze', 'stg_dim_geographic_entity') }} as municipio,
    {{ source('aq360_bronze', 'stg_dim_geographic_entity') }} as pedania,
    {{ source('aq360_bronze', 'stg_dim_geographic_entity') }} as poblacion,
    bvsdteg
where mundo.[ENTITY_TYPE] = 0
    AND pais.ID_GEO_ENTITY_P = mundo.ID_GEO_ENTITY
    AND CCAA.ID_GEO_ENTITY_P = pais.ID_GEO_ENTITY
    AND provincia.ID_GEO_ENTITY_P = CCAA.ID_GEO_ENTITY
    AND municipio.ID_GEO_ENTITY_P = provincia.ID_GEO_ENTITY
    AND pedania.ID_GEO_ENTITY_P = municipio.ID_GEO_ENTITY
    AND poblacion.ID_GEO_ENTITY_P = pedania.ID_GEO_ENTITY
    and poblacion.ID_GEO_ENTITY = bvsdteg.ID_GEOENTITY
    and mundo.FLG_ACTV = 1
    and pais.FLG_ACTV = 1
    and CCAA.FLG_ACTV = 1
    and provincia.FLG_ACTV = 1
    and municipio.FLG_ACTV = 1
    and pedania.FLG_ACTV = 1
    and poblacion.FLG_ACTV = 1

