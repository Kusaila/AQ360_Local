select ID_COMPANY cd_explotacion,
    TIPO_PERIODO TipoPeriodo,
    spe.Anio,
    spe.InicioMes Fecha_ini_periodo,
    spe.FinMes Fecha_fin_periodo,
    spe.Dias,
    sdp.CdPeriodo,
    sdp.DsTipPeriodo,
    sdemtp.FEC_DESDE Fecha_ini_vig,
    sdemtp.FEC_HASTA Fecha_fin_vig
from {{ source('aq360_bronze', 'stg_DIM_EXPLO_MANUAL_TIP_PER') }} sdemtp
    inner join {{ ref('slv_periodos_exploman') }} spe on sdemtp.TIPO_PERIODO = spe.TipoPeriodo
    inner join {{ ref('slv_dim_periodo') }} sdp on spe.Periodo = sdp.DsTipPeriodo
    and spe.Anio = sdp.Anno
    and spe.CdPeriodo = sdp.CdPeriodo