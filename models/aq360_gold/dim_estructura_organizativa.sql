select 
id sk_explotacion,
sk_explotacion cd_explotacion,
des_ambito,
des_zona,
des_delegacion,
des_ugestion,
des_contrata,
des_explotacion,
Tipo_Org,
(YEAR(dbt_valid_from) * 100) + MONTH(dbt_valid_from) Fech_mes_ini,
case 
	when dbt_valid_to is null
	then 999912
	else (YEAR(dbt_valid_to) * 100) + MONTH(dbt_valid_to)
end Fech_mes_fin
from {{ ref("dim_estructura_organizativa_hist") }}