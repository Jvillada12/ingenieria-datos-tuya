-- =====================================================
-- ESQUEMA DE BASE DE DATOS PARA ANÁLISIS DE RACHAS
-- =====================================================

-- Eliminar vistas y tablas si existen (para poder recrear)
DROP VIEW IF EXISTS v_clientes_estadisticas;
DROP VIEW IF EXISTS v_historia_con_niveles;
DROP TABLE IF EXISTS rachas_resultado;
DROP TABLE IF EXISTS historia_completa;
DROP TABLE IF EXISTS retiros;
DROP TABLE IF EXISTS historia;

-- =====================================================
-- TABLA: historia
-- Datos originales de saldos por cliente y mes
-- =====================================================
CREATE TABLE historia (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    identificacion TEXT NOT NULL,
    corte_mes DATE NOT NULL,
    saldo DECIMAL(15,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- TABLA: retiros  
-- Fechas de retiro de clientes
-- =====================================================
CREATE TABLE retiros (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    identificacion TEXT NOT NULL,
    fecha_retiro DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- TABLA: historia_completa
-- Serie temporal completa con niveles calculados
-- =====================================================
CREATE TABLE historia_completa (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    identificacion TEXT NOT NULL,
    corte_mes DATE NOT NULL,
    saldo DECIMAL(15,2) NOT NULL,
    nivel TEXT NOT NULL CHECK (nivel IN ('N0', 'N1', 'N2', 'N3', 'N4')),
    es_real BOOLEAN NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- TABLA: rachas_resultado
-- Resultado final del análisis de rachas
-- =====================================================
CREATE TABLE rachas_resultado (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    identificacion TEXT NOT NULL,
    racha INTEGER NOT NULL,
    fecha_fin DATE NOT NULL,
    nivel TEXT NOT NULL CHECK (nivel IN ('N0', 'N1', 'N2', 'N3', 'N4')),
    fecha_base DATE NOT NULL,
    min_racha INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- ÍNDICES PARA OPTIMIZAR CONSULTAS
-- =====================================================

-- Índices para tabla historia
CREATE INDEX idx_historia_identificacion ON historia(identificacion);
CREATE INDEX idx_historia_corte_mes ON historia(corte_mes);
CREATE INDEX idx_historia_cliente_fecha ON historia(identificacion, corte_mes);

-- Índices para tabla retiros
CREATE INDEX idx_retiros_identificacion ON retiros(identificacion);
CREATE INDEX idx_retiros_fecha ON retiros(fecha_retiro);

-- Índices para tabla historia_completa
CREATE INDEX idx_completa_identificacion ON historia_completa(identificacion);
CREATE INDEX idx_completa_corte_mes ON historia_completa(corte_mes);
CREATE INDEX idx_completa_nivel ON historia_completa(nivel);
CREATE INDEX idx_completa_cliente_fecha ON historia_completa(identificacion, corte_mes);

-- Índices para tabla rachas_resultado
CREATE INDEX idx_resultado_identificacion ON rachas_resultado(identificacion);
CREATE INDEX idx_resultado_racha ON rachas_resultado(racha);
CREATE INDEX idx_resultado_nivel ON rachas_resultado(nivel);

-- =====================================================
-- FUNCIÓN PARA CLASIFICAR NIVELES DE SALDO
-- =====================================================

-- Nota: SQLite no soporta funciones definidas por usuario
-- Esta lógica se implementará en las consultas usando CASE WHEN

-- Niveles de clasificación:
-- N0: Saldo >= 0 y < 300,000
-- N1: Saldo >= 300,000 y < 1,000,000  
-- N2: Saldo >= 1,000,000 y < 3,000,000
-- N3: Saldo >= 3,000,000 y < 5,000,000
-- N4: Saldo >= 5,000,000

-- =====================================================
-- VISTA: v_historia_con_niveles
-- Historia con niveles calculados automáticamente
-- =====================================================
CREATE VIEW v_historia_con_niveles AS
SELECT 
    h.id,
    h.identificacion,
    h.corte_mes,
    h.saldo,
    CASE 
        WHEN h.saldo >= 0 AND h.saldo < 300000 THEN 'N0'
        WHEN h.saldo >= 300000 AND h.saldo < 1000000 THEN 'N1'
        WHEN h.saldo >= 1000000 AND h.saldo < 3000000 THEN 'N2'
        WHEN h.saldo >= 3000000 AND h.saldo < 5000000 THEN 'N3'
        WHEN h.saldo >= 5000000 THEN 'N4'
        ELSE 'ERROR'
    END AS nivel,
    h.created_at
FROM historia h;

-- =====================================================
-- VISTA: v_clientes_estadisticas
-- Estadísticas básicas por cliente
-- =====================================================
CREATE VIEW v_clientes_estadisticas AS
SELECT 
    identificacion,
    COUNT(*) as total_registros,
    MIN(corte_mes) as primera_fecha,
    MAX(corte_mes) as ultima_fecha,
    MIN(saldo) as saldo_minimo,
    MAX(saldo) as saldo_maximo,
    AVG(saldo) as saldo_promedio,
    COUNT(DISTINCT 
        CASE 
            WHEN saldo >= 0 AND saldo < 300000 THEN 'N0'
            WHEN saldo >= 300000 AND saldo < 1000000 THEN 'N1'
            WHEN saldo >= 1000000 AND saldo < 3000000 THEN 'N2'
            WHEN saldo >= 3000000 AND saldo < 5000000 THEN 'N3'
            WHEN saldo >= 5000000 THEN 'N4'
        END
    ) as niveles_diferentes
FROM historia
GROUP BY identificacion;

-- =====================================================
-- COMENTARIOS Y DOCUMENTACIÓN
-- =====================================================

/*
DOCUMENTACIÓN DEL ESQUEMA:

1. TABLA historia:
   - Contiene los datos originales del archivo Excel
   - Cada registro representa el saldo de un cliente en un mes específico
   
2. TABLA retiros:
   - Contiene las fechas de retiro de clientes
   - Usado para determinar cuándo dejar de considerar a un cliente
   
3. TABLA historia_completa:
   - Serie temporal completa con registros interpolados
   - Si un cliente no aparece en un mes, se asigna nivel N0
   - Respeta las fechas de retiro
   
4. TABLA rachas_resultado:
   - Resultado final del análisis
   - Una fila por cliente con su mejor racha
   
5. VISTAS:
   - v_historia_con_niveles: Historia con clasificación automática
   - v_clientes_estadisticas: Estadísticas resumidas por cliente

REGLAS DE NEGOCIO IMPLEMENTADAS:
- Clasificación automática en niveles N0-N4
- Interpolación de meses faltantes como N0
- Respeto a fechas de retiro
- Selección de racha más larga por cliente
- En caso de empate, se elige la más reciente
*/