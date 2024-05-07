    select id_company as sk_explotacion,
    case
        when CHARINDEX('AG Zona', DESCRIPTION_ZONA) !=0 then 'Nacional'
        when DESCRIPTION_ZONA ='AG América' then 'Internacional'
        when DESCRIPTION_ZONA ='AG Estructura Internacional' then 'Internacional'
        when DESCRIPTION_ZONA ='AG Europa' then 'Internacional'
        when DESCRIPTION_ZONA ='AG Mena' then 'Internacional'        
    else 'Sin Indicar' end des_ambito,
    DESCRIPTION_ZONA as des_zona,
    DESCRIPTION_DELEGACION as des_delegacion,
    DESCRIPTION_UNIDAD_GESTION as des_ugestion,
    DESCRIPTION_CONTRATA as des_contrata,
    DESCRIPTION_EXPLOTACION as des_explotacion
     from {{ ref('slv_dim_estructura_organizativa_hist') }}

