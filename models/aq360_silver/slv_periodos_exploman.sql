WITH Anios AS (
    SELECT DISTINCT YEAR(Fecha_date) AS Anio
    FROM {{ source('aq360_silver', 'slv_dim_Fecha') }}
),
Meses AS (
    SELECT Anio,
        Mes,
        DATEADD(MONTH, Mes - 1, DATEFROMPARTS(Anio, 1, 1)) AS InicioMes,
        DATEADD(DAY,-1,DATEADD(MONTH, Mes, DATEFROMPARTS(Anio, 1, 1))) AS FinMes,
        DAY(DATEADD(DAY,-1,DATEADD(MONTH, Mes, DATEFROMPARTS(Anio, 1, 1)))) Dias
    FROM Anios
        CROSS JOIN (
            VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12)
        ) AS Meses(Mes)
),
Bimestres AS (
    SELECT Anio,
        (Mes + 1) / 2 AS Bimestre,
        MIN(InicioMes) AS InicioBimestre,
        MAX(FinMes) AS FinBimestre,
        DATEDIFF(DAY,MIN(InicioMes), MAX(FinMes)) Dias
    FROM Meses
    GROUP BY Anio,
        (Mes + 1) / 2
),
Trimestres AS (
    SELECT Anio,
        (Mes + 2) / 3 AS Trimestre,
        MIN(InicioMes) AS InicioTrimestre,
        MAX(FinMes) AS FinTrimestre,
                DATEDIFF(DAY,MIN(InicioMes), MAX(FinMes)) Dias
    FROM Meses
    GROUP BY Anio,
        (Mes + 2) / 3
),
Cuatrimestres AS (
    SELECT Anio,
        (Mes + 3) / 4 AS Cuatrimestre,
        MIN(InicioMes) AS InicioCuatrimestre,
        MAX(FinMes) AS FinCuatrimestre,
        DATEDIFF(DAY,MIN(InicioMes), MAX(FinMes)) Dias
    FROM Meses
    GROUP BY Anio,
        (Mes + 3) / 4
),
Semestres AS (
    SELECT Anio,
        (Mes + 5) / 6 AS Semestre,
        MIN(InicioMes) AS InicioSemestre,
        MAX(FinMes) AS FinSemestre,
        DATEDIFF(DAY,MIN(InicioMes), MAX(FinMes)) Dias        
    FROM Meses
    GROUP BY Anio,
        (Mes + 5) / 6
),
Anuales AS (
    SELECT Anio,
        MIN(InicioMes) AS InicioAnual,
        MAX(FinMes) AS FinAnual,
        DATEDIFF(DAY,MIN(InicioMes), MAX(FinMes))+1 Dias
    FROM Meses
    GROUP BY Anio
)
SELECT Anio,
    InicioMes,
    FinMes,
    Dias,
    'Mensual' AS Periodo,
    Mes as Orden,
    cast(Anio as varchar)+'1'+RIGHT('0' + CAST(Mes AS VARCHAR(2)), 2) AS CdPeriodo,
    'TIPER00001' as TipoPeriodo
FROM Meses
UNION ALL
SELECT Anio,
    InicioBimestre,
    FinBimestre,
    Dias,
    'Bimestral' AS Periodo,
    Bimestre as Orden,
    cast(Anio as varchar)+'2'+RIGHT('0' + CAST(Bimestre AS VARCHAR(2)), 2) AS CdPeriodo,
    'TIPER00002' as TipoPeriodo
FROM Bimestres
UNION ALL
SELECT Anio,
    InicioTrimestre,
    FinTrimestre,
    Dias,    
    'Trimestral' AS Periodo,
    Trimestre as Orden,
    cast(Anio as varchar)+'3'+RIGHT('0' + CAST(Trimestre AS VARCHAR(2)), 2) AS CdPeriodo,
    'TIPER00003' as TipoPeriodo
FROM Trimestres
UNION ALL
SELECT Anio,
    InicioCuatrimestre,
    FinCuatrimestre,
    Dias,    
    'Cuatrimestral' AS Periodo,
    Cuatrimestre as Orden,
    cast(Anio as varchar)+'4'+RIGHT('0' + CAST(Cuatrimestre AS VARCHAR(2)), 2) AS CdPeriodo,  
    'TIPER00004' as TipoPeriodo
FROM Cuatrimestres
UNION ALL
SELECT Anio,
    InicioSemestre,
    FinSemestre,
    Dias,    
    'Semestral' AS Periodo,
    Semestre as Orden,
    cast(Anio as varchar)+'5'+RIGHT('0' + CAST(Semestre AS VARCHAR(2)), 2) AS CdPeriodo,      
    'TIPER00005' as TipoPeriodo
FROM Semestres
UNION ALL
SELECT Anio,
    InicioAnual,
    FinAnual,
    Dias,    
    'Anual' AS Periodo,
    1 as Orden,
    cast(Anio as varchar)+'601' as CdPeriodo,
    'TIPER00006' as TipoPeriodo
FROM Anuales