with slv_dim_estructura_societaria as (
SELECT
        dves.ID_COMPANY     ,
        dves.NAME_TYPE      ,
        dves.FISCAL_NUMBER  ,
        dtme.cdtipo         ,
        dtme.Proporcional   ,
        dtme.Consolidado
        FROM
        {{ ref('slv_dw_v_Estructura_Societaria') }} dves
left outer join
        (
                select
                        CdEmpresa  ,
                        gnNif,
                        CdTipo      ,
                        Proporcional,
                        Consolidado ,
                        max(Update_Date) maxupd
                from
                        {{ source('aq360_bronze','stg_dw_t_mEmpresas_Evol') }} dtme
                where dtme.dFech_Fin is null        
                group by
                        CdEmpresa  ,
                        gnNif,
                        CdTipo      ,
                        Proporcional,
                        Consolidado) dtme
on
        dves.COD_COMPANY = dtme.CdEmpresa
        and dves.FISCAL_NUMBER = dtme.gnNif
where
        dves.to_date is null
)
select distinct
        ID_COMPANY     sk_empresa,
        FISCAL_NUMBER  cif,
        case when cdtipo= 0 then 'Propia' else 'Ajena' end as TipoEmpresa,
        NAME_TYPE      nombre_empresa
from slv_dim_estructura_societaria