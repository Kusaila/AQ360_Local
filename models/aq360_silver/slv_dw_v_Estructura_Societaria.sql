with gcer as (
    select *
    from {{ source(
            'aq360_bronze',
            'stg_gcrep_company_explo_relation'
        ) }}
),
gc as (
    select *
    from {{ source('aq360_bronze', 'stg_gcrep_company') }}
)
SELECT gcer.[ID_EXPLOITATION],
    gcer.[ID_COMPANY],
    gc.COD_COMPANY,
    gc.NAME_TYPE,
    gc.FISCAL_NUMBER,
    gcer.[COD_RELATION_TYPE],
    case
        when gcer.[COD_RELATION_TYPE] = '0001CREL_T' then 'Principal'
        when gcer.[COD_RELATION_TYPE] = '0002CREL_T' then 'Emisora'
        else 'Otra'
    end as Company_Type,
    gcer.[FROM_DATE],
    gcer.[TO_DATE]
FROM gcer,
    gc
WHERE gcer.[FLG_ACTV] = 1
    and gc.ID_COMPANY = gcer.ID_COMPANY