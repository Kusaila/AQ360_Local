select
        CdTipCli                             ,
        DsTipcli                             ,
        dtmdatc.cod_agrp_tip_cli CdAgrpTipCli,
        dtmmatc.des_agrp_tip_cli DsAgrpTipCli,
        case 
	        when dtmdatc.cod_agrp_tip_cli = 3 then 1
	        when dtmdatc.cod_agrp_tip_cli = 2 then 3
	        when dtmdatc.cod_agrp_tip_cli = 1 then 2
        end id_alta_baja, 
        dtmdatc.fech_ini_det_agr_tipcli fec_ini_vig,
        dtmdatc.fech_fin_det_agr_tipcli fec_fin_vig
from
         {{ source('aq360_bronze', 'stg_dw_t_mTipoCliente') }} dtmtc
inner join
         {{ source('aq360_bronze', 'stg_dw_t_md_det_agrup_tip_cli') }} dtmdatc
on
        dtmtc.CdTipCli = dtmdatc.cod_tip_cli
inner join
        {{ source('aq360_bronze', 'stg_dw_t_md_mae_agrup_tip_cli') }} dtmmatc
on
        dtmdatc.cod_agrp_tip_cli = dtmmatc.cod_agrp_tip_cli
