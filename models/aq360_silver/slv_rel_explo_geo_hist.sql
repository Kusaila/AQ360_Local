select sdteg.CdPoblacion,
    sdge.ID_COMPANY CdExplotacion,
    sdge.FECHA_INI_ACTV,
    sdge.FECHA_FIN_ACTV,
    sdge.FLG_ACTV
from {{ source('aq360_bronze', 'stg_DIM_GEO_EXPLO') }}  sdge
    inner join (
        select distinct ID_GEOENTITY,
            CdPoblacion
        from {{ source('aq360_bronze', 'stg_dw_t_eqGeografica') }}
    ) sdteg on sdge.ID_GEO_ENTITY = sdteg.ID_GEOENTITY