select sAnno,
    cdTipPeriodo,
    DsTipPeriodo,
    Anno,
    NumDias,
    Periodo,
    Periodo_ant,
    sAnno + '_' + UPPER(SUBSTRING(DsTipPeriodo, 1, 2)) + '_' + substring(Periodo,6,2) periodo_des
from {{ ref("slv_dim_periodo") }}