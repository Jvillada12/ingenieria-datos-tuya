-- =====================================================
-- CONSULTA PRINCIPAL PARA CÁLCULO DE RACHAS
-- =====================================================

WITH RECURSIVE 
-- CTE 1: Base de datos con números de fila para detectar secuencias
datos_numerados AS (
    SELECT 
        identificacion,
        corte_mes,
        nivel,
        ROW_NUMBER() OVER (PARTITION BY identificacion ORDER BY corte_mes) as rn
    FROM historia_completa
    WHERE corte_mes <= '2024-12-31'
),

-- CTE 2: Detectar cambios de nivel para identificar grupos de rachas
cambios_nivel AS (
    SELECT 
        identificacion,
        corte_mes,
        nivel,
        rn,
        CASE 
            WHEN LAG(nivel) OVER (PARTITION BY identificacion ORDER BY corte_mes) != nivel 
                OR LAG(nivel) OVER (PARTITION BY identificacion ORDER BY corte_mes) IS NULL
            THEN 1 
            ELSE 0 
        END as cambio
    FROM datos_numerados
),

-- CTE 3: Asignar grupo de racha a cada registro
grupos_racha AS (
    SELECT 
        identificacion,
        corte_mes,
        nivel,
        rn,
        SUM(cambio) OVER (PARTITION BY identificacion ORDER BY corte_mes) as grupo_racha
    FROM cambios_nivel
),

-- CTE 4: Calcular estadísticas de cada racha
estadisticas_rachas AS (
    SELECT 
        identificacion,
        nivel,
        grupo_racha,
        COUNT(*) as longitud_racha,
        MIN(corte_mes) as fecha_inicio,
        MAX(corte_mes) as fecha_fin
    FROM grupos_racha
    GROUP BY identificacion, nivel, grupo_racha
    HAVING COUNT(*) >= 3  -- Mínimo 3 meses
),

-- CTE 5: Ranking de rachas por cliente (más larga primero, luego más reciente)
ranking_rachas AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY identificacion 
            ORDER BY longitud_racha DESC, fecha_fin DESC
        ) as ranking
    FROM estadisticas_rachas
)

-- Selección final: mejor racha por cliente
SELECT 
    identificacion,
    longitud_racha as racha,
    fecha_fin,
    nivel
FROM ranking_rachas
WHERE ranking = 1
ORDER BY racha DESC, fecha_fin DESC;