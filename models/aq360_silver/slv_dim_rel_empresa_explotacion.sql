SELECT DISTINCT 
concat(sdes.sk_empresa, sdeo.sk_explotacion) cd_empr_explo,
cast(sdes.sk_empresa as bigint) sk_empresa,
cast(sdeo.sk_explotacion as bigint) sk_explotacion,
sdes.TipoEmpresa,
sdes.nombre_empresa,
sdeo.des_ambito,
sdeo.des_zona,
sdeo.des_delegacion,
sdeo.des_ugestion ,
sdeo.des_contrata,
sdeo.des_explotacion
from {{ ref("slv_dim_estructura_societaria") }} sdes
inner join {{ ref('slv_dw_v_Estructura_Societaria') }} sdves
on sdes.sk_empresa = sdves.ID_COMPANY
inner join {{ ref("slv_dim_estructura_organizativa_hist") }} sdeo 
on sdves.ID_EXPLOITATION = sdeo.sk_explotacion
where (sdves.FROM_DATE <= FORMAT(CURRENT_TIMESTAMP,'yyyyMMdd')
and sdves.TO_DATE > FORMAT(CURRENT_TIMESTAMP,'yyyyMMdd') or sdves.TO_DATE is null)
and Tipo_Org = 'Tarifaria'