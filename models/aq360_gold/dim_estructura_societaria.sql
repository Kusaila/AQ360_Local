select 
id sk_empresa,
sk_empresa cd_empresa,
cif,
TipoEmpresa,
nombre_empresa,
(YEAR(dbt_valid_from) * 100) + MONTH(dbt_valid_from) Fech_mes_ini,
case 
	when dbt_valid_to is null
	then 999912
	else (YEAR(dbt_valid_to) * 100) + MONTH(dbt_valid_to)
end Fech_mes_fin
from {{ ref("dim_estructura_societaria_hist") }}