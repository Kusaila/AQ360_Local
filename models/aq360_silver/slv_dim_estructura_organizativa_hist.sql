{{
  config(
    materialized = 'table',
    test = 'unique'
    )
}}
SELECT DOS.[ID_COMPANY] as sk_explotacion,
    case
        when CHARINDEX('AG Zona', DOSDZ.[DESCRIPTION]) !=0 then 'Nacional'
        when DOSDZ.[DESCRIPTION] ='AG América' then 'Internacional'
        when DOSDZ.[DESCRIPTION] ='AG Estructura Internacional' then 'Internacional'
        when DOSDZ.[DESCRIPTION] ='AG Europa' then 'Internacional'
        when DOSDZ.[DESCRIPTION] ='AG Mena' then 'Internacional'        
    else 'Sin Indicar' end des_ambito,
    DOSDZ.[DESCRIPTION] as des_zona,
    DOSDD.[DESCRIPTION] as des_delegacion,
    DOSDUG.[DESCRIPTION] as des_ugestion,
    DOSDC.[DESCRIPTION] as des_contrata,
    DOSDE.[DESCRIPTION] as des_explotacion,
    IsNull((SELECT case when gc.COMPANY_TYPE = 'COTYPE0005' Then 'Tarifaria'
	               else case when DOS.ID_COMPANY between 1200000000 and 1299999999 then 'No Tarifaria'
				             when DOS.ID_COMPANY between 2000000000 and 2099999999 then 'Servicio AT'
							 else 'Otro'
				       end
			  end
		 FROM {{ source('aq360_bronze', 'stg_gcrep_company') }} gc WHERE gc.Id_company = DOS.ID_COMPANY),'Otro') as Tipo_Org
    FROM {{ source('aq360_bronze', 'stg_dim_org_structure') }} DOS,
     {{ source('aq360_bronze', 'stg_dim_org_structure_desc') }} DOSDZ,
     {{ source('aq360_bronze', 'stg_dim_org_structure_desc') }} DOSDD,
     {{ source('aq360_bronze', 'stg_dim_org_structure_desc') }} DOSDUG,
     {{ source('aq360_bronze', 'stg_dim_org_structure_desc') }} DOSDC,
     {{ source('aq360_bronze', 'stg_dim_org_structure_desc') }} DOSDS,
     {{ source('aq360_bronze', 'stg_dim_org_structure_desc') }} DOSDE,
     {{ source('aq360_bronze', 'stg_dim_org_structure_desc') }} DOSDDI
WHERE DOS.[ID_ZONA] = DOSDZ.ID_ORGANIZATIONAL_AREA
    AND DOS.[ID_DELEGACION] = DOSDD.ID_ORGANIZATIONAL_AREA
    AND DOS.[ID_UNIDAD_GESTION] = DOSDUG.ID_ORGANIZATIONAL_AREA
    AND DOS.[ID_CONTRATA] = DOSDC.ID_ORGANIZATIONAL_AREA
    AND DOS.[ID_SERVICIO] = DOSDS.ID_ORGANIZATIONAL_AREA
    AND DOS.[ID_EXPLOTACION] = DOSDE.ID_ORGANIZATIONAL_AREA
    AND DOS.[ID_DIVISION] = DOSDDI.ID_ORGANIZATIONAL_AREA
    AND DOS.FLG_ACTV = 1
    AND DOSDZ.FLG_ACTV = 1
    AND DOSDD.FLG_ACTV = 1
    AND DOSDUG.FLG_ACTV = 1
    AND DOSDC.FLG_ACTV = 1
    AND DOSDS.FLG_ACTV = 1
    AND DOSDE.FLG_ACTV = 1
    AND DOSDDI.FLG_ACTV = 1
    AND DOS.[nid] = (
        Select TOP 1 (UDOS.[nid])
        From  {{ source('aq360_bronze', 'stg_dim_org_structure') }} as UDOS
        WHERE UDOS.[ID_COMPANY] = DOS.[ID_COMPANY]
            and FLG_ACTV = 1
        order by VALID_FROM DESC
    )