select 
dtej.nCod_inst,
dtej.nCod_expl,
dtej.nCod_pobl,
dtej.dFech_ini_del,
dtej.dFech_fin_del, 
dtej.nCod_emi,
dtej.CdDelegacioncCont,
dtej.ID_COMPANY sk_empresa
from {{ source('aq360_bronze', 'stg_dw_t_eqJuridica') }} dtej
where dFech_fin_del is null 
