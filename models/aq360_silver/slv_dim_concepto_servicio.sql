SELECT
        dtmc.cod_inst  ,
        dtmc.cod_ctta  ,
        dtmc.cod_concep,
        dtmc.des_concep,
        dtmc.cod_ser   ,
        dtms.des_ser   ,
        case
        when
                charindex(UPPER('Canon'),UPPER(des_ser)) != 0
        then
                1
        else
                0
        end flag_canon,
        dtmc.Create_Date,
        dtmc.Update_Date 
FROM
        {{ source('aq360_bronze', 'stg_dw_t_md_conceptos') }} dtmc
inner join
        {{ source('aq360_bronze', 'stg_dw_t_md_servicios') }} dtms
on
        dtmc.cod_ser  = dtms.cod_ser
and     dtmc.cod_inst = dtms.cod_inst