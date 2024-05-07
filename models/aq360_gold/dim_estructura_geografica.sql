select 
id sk_poblacion,
sk_poblacion cd_poblacion,
des_pais,
des_ccaa,
des_provincia,
des_municipio,
des_pedania,
des_poblacion,
(YEAR(dbt_valid_from) * 100) + MONTH(dbt_valid_from) Fech_mes_ini,
case 
	when dbt_valid_to is null
	then 999912
	else (YEAR(dbt_valid_to) * 100) + MONTH(dbt_valid_to)
end Fech_mes_fin
from {{ ref("dim_estructura_geografica_hist") }}