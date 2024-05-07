SELECT distinct Fech_mes,
    'aq360_bronze' esquema,
    'stg_tFacServConcep' tabla,
    ingestDate
FROM {{ source('aq360_bronze', 'stg_tFacServConcep') }}
where ingestDate = (
        select max(ingestDate)
        from {{ source('aq360_bronze', 'stg_tFacServConcep') }}
    )
union
SELECT distinct Fech_mes,
    'aq360_bronze' esquema,
    'stg_tFacturas2' tabla,
    ingestDate
FROM {{ source('aq360_bronze', 'stg_tFacturas2') }}
where ingestDate = (
        select max(ingestDate)
        from {{ source('aq360_bronze', 'stg_tFacturas2') }}
    )
union
SELECT distinct ANO_MES Fech_mes,
    'aq360_bronze' esquema,
    'stg_tExploMan_Datos_Basicos' tabla,
    ingestDate
FROM {{ source('aq360_bronze', 'stg_tExploMan_Datos_Basicos') }}
where ingestDate = (
        select max(ingestDate)
        from {{ source('aq360_bronze', 'stg_tExploMan_Datos_Basicos') }}
    )
and ANO_MES is not null
union
SELECT distinct PERIODO_ANALISIS Fech_mes,
    'aq360_bronze' esquema,
    'stg_tExploMan_FactServConc_GrupSeg' tabla,
    ingestDate
FROM {{ source('aq360_bronze', 'stg_tExploMan_FactServConc_GrupSeg') }}
where ingestDate = (
        select max(ingestDate)
        from {{ source('aq360_bronze', 'stg_tExploMan_FactServConc_GrupSeg') }}
    )
and PERIODO_ANALISIS is not null