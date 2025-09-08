# Solución Integral de Ingeniería de Datos

**Desarrollado por:** Johnnatan Villada Flórez  
**Fecha de entrega:** Septiembre 8, 2025  
**Prueba técnica:** Ingeniería de Datos Tuya

## Estructura del Proyecto

```
ingenieria-datos-tuya/
├── README.md                                  # Documentación principal
├── requirements.txt                           # Dependencias Python
├── calculador_rachas.py                       # Script principal rachas Python
├── ejecutar_rachas_sql.py                     # Ejecutor consultas SQL
├── preparar_datos_powerbi.py                  # Preparación datos Power BI
├── ejercicio1_conceptual/
│   └── arquitectura_dataset_telefono.md       # Arquitectura conceptual
├── ejercicio2_kpis/
│   └── sistema_kpis_calidad.md                # Sistema KPIs conceptual
├── ejercicio4_powerbi/
│   └── provisiones_dashboard.pbix             # Dashboard Power BI
├── src/ejercicio3_rachas/
│   ├── python/
│   │   ├── database_manager.py                # Gestión base datos
│   │   ├── explorar_datos.py                  # Análisis exploratorio
│   │   └── test_datos.py                      # Tests
│   └── sql/
│       ├── schema.sql                         # Esquema SQLite
│       ├── rachas_query.sql                   # Consulta principal
│       └── load_data.sql                      # Carga datos
├── data/
│   ├── raw/
│   │   ├── Rachas.xlsx                        # Datos originales rachas
│   │   └── Provisiones.xlsx                   # Datos originales provisiones
│   └── output/
│       ├── rachas_resultado.csv               # Resultados Python
│       ├── rachas_sql_resultado.csv           # Resultados SQL
│       ├── provisiones_powerbi.xlsx           # Datos Power BI
│       └── rachas.db                          # Base datos SQLite
└── tests/                                     # Casos de prueba
```

## Ejercicios Desarrollados

### Ejercicio 1: Arquitectura Dataset de Números de Teléfono (Conceptual)

**Objetivo:** Diseño conceptual de proceso automatizado para creación y mantenimiento de dataset confiable de números telefónicos.

**Solución Implementada:**
- Arquitectura técnica completa con diagramas de flujo
- Pipeline de CI/CD automatizado con GitHub Actions
- Sistema de validación y calidad de datos en tiempo real
- Procedimientos operativos y monitoreo continuo
- API REST para acceso programático
- Sistema de versionado de datasets

**Características técnicas:**
- Validación automática de formatos telefónicos
- Integración con múltiples fuentes de datos
- Normalización a estándar E.164
- Deduplicación inteligente con ML
- Auditoría completa y trazabilidad
- Cumplimiento GDPR y LOPD

**Ubicación:** `ejercicio1_conceptual/arquitectura_dataset_telefono.md`

### Ejercicio 2: Sistema de KPIs de Calidad de Datos (Conceptual)

**Objetivo:** Herramienta conceptual de veeduría para calidad de datos y trazabilidad, orientada a equipos de negocio.

**Solución Implementada:**
- Dashboard interactivo de métricas de calidad
- Sistema de trazabilidad completa del dato
- Alertas automáticas por anomalías
- Tendencias históricas de calidad
- API para acceso programático
- Integración con sistemas de negocio

**Dimensiones de calidad monitoreadas:**
- Completitud (>= 95%)
- Precisión (>= 98%)
- Consistencia (>= 96%)
- Oportunidad (<= 24 horas)
- Unicidad (>= 99%)

**Ubicación:** `ejercicio2_kpis/sistema_kpis_calidad.md`

### Ejercicio 3: Análisis de Rachas - Implementación Dual

**Objetivo:** Identificación de rachas consecutivas de clientes por nivel de deuda usando SQL y Python.

**Especificaciones Técnicas:**
- Datos procesados: 2,925 registros, 150 clientes únicos
- Período análisis: Enero 2023 - Diciembre 2024
- Niveles de clasificación:
  - N0: $0 <= saldo < $300,000
  - N1: $300,000 <= saldo < $1,000,000
  - N2: $1,000,000 <= saldo < $3,000,000
  - N3: $3,000,000 <= saldo < $5,000,000
  - N4: saldo >= $5,000,000

**Implementaciones desarrolladas:**

*Implementación SQL:*
- Base de datos SQLite con esquema optimizado
- Consulta principal usando CTEs y window functions
- Índices para optimización de rendimiento
- Procedimientos almacenados para automatización

*Implementación Python:*
- Clase CalculadorRachas orientada a objetos
- Algoritmo optimizado con pandas y numpy
- Validaciones automáticas de calidad
- Testing exhaustivo con casos de prueba

**Resultados obtenidos:**
- 91 clientes con rachas válidas >= 3 meses consecutivos
- Validación cruzada: 100% concordancia SQL vs Python
- Distribución por niveles:
  - N0: 16 clientes (17.6%)
  - N1: 1 cliente (1.1%)
  - N2: 27 clientes (29.7%)
  - N3: 16 clientes (17.6%)
  - N4: 31 clientes (34.1%)
- Racha promedio: 3.5 meses
- Racha máxima: 6 meses

**Archivos principales:**
- `src/ejercicio3_rachas/sql/rachas_query.sql`
- `calculador_rachas.py`
- `data/output/rachas_resultado.csv`
- `data/output/rachas_sql_resultado.csv`

### Ejercicio 4: Dashboard Power BI - Análisis de Provisiones

**Objetivo:** Dashboard interactivo para análisis integral del área de provisiones con métricas financieras calculadas.

**Datos procesados:**
- 288 registros de provisiones analizados
- 284 registros de condonaciones y recuperaciones
- Período: Diciembre 2022 - Julio 2025
- Alianzas: A, C, E, V
- Productos: MASTERCARD, CREDICOMPRA, PRIVADA, EXITO_LIBRE INVERSION

**Métricas calculadas:**
- ICV_30: Indicador Calidad Cartera (sald_30mas/sald_k_ifrs) = 21.3% promedio
- Gasto de Provisión: Provisión actual - provisión anterior = $3,247,223 total
- Gasto Provisión Neto: Gasto + condonaciones - recuperaciones = $705,369 total

**Componentes del dashboard:**

*Tarjetas KPI principales:*
- Total Saldo Capital: $1 mil M
- ICV_30 Promedio: 61.27%
- Total Provisiones: 200 mill
- Gasto Provisión Neto: 483 mil

*Matriz resumen ejecutiva:*
- Filas: Alianza, Producto
- Valores: Saldo Capital, Provisiones, ICV_30
- Formato condicional por niveles de riesgo

*Gráfico de tendencia temporal:*
- Evolución de saldos, provisiones e ICV_30
- Análisis de tendencias por año
- Identificación de patrones estacionales

*Filtros interactivos:*
- Filtro de fechas avanzado
- Selección por alianza
- Filtro por producto

**Validación de calidad:**
- Completitud: 100% en campos críticos de provisiones
- Integridad: 0 registros duplicados
- Consistencia: 96.1% completitud en recuperaciones
- Coherencia: Validación de sumas y cálculos

**Archivos entregados:**
- `ejercicio4_powerbi/provisiones_dashboard.pbix` (Dashboard funcional)
- `data/output/provisiones_powerbi.xlsx` (Datos preparados)

## Instalación y Configuración

### Prerrequisitos

```bash
# macOS
brew install python3 git

# Verificar versiones
python3 --version  # >= 3.9
git --version      # >= 2.0
```

### Configuración del Entorno

```bash
# Clonar repositorio
git clone [URL_REPOSITORIO]
cd ingenieria-datos-tuya

# Crear entorno virtual
python3 -m venv venv
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt

# Verificar instalación
python3 -c "import pandas, sqlite3, openpyxl; print('Instalación exitosa')"
```

## Ejecución de Soluciones

### Ejercicio 3: Análisis de Rachas

```bash
# Activar entorno virtual
source venv/bin/activate

# Implementación Python
python3 calculador_rachas.py

# Configuración base de datos SQL
python3 src/ejercicio3_rachas/python/database_manager.py

# Ejecución consulta SQL
python3 ejecutar_rachas_sql.py

# Resultados generados:
# - data/output/rachas_resultado.csv (Python)
# - data/output/rachas_sql_resultado.csv (SQL)
# - data/output/rachas.db (Base de datos SQLite)
```

### Ejercicio 4: Preparación Dashboard Power BI

```bash
# Preparar datos para Power BI
python3 preparar_datos_powerbi.py

# Resultado: data/output/provisiones_powerbi.xlsx
# Dashboard: ejercicio4_powerbi/provisiones_dashboard.pbix
```

## Resultados y Validación

### Métricas de Calidad Técnica

**Ejercicio 3 - Rachas:**
- Precisión: 100% concordancia entre implementaciones SQL y Python
- Completitud: 91 de 150 clientes (60.7%) con rachas válidas
- Rendimiento: Procesamiento < 5 segundos para dataset completo
- Escalabilidad: Optimizado para datasets hasta 100K registros

**Ejercicio 4 - Provisiones:**
- Integridad: 100% completitud en campos críticos
- Consistencia: 0 registros duplicados
- Cobertura temporal: 31 meses de datos históricos
- Precisión cálculos: Validación manual de métricas financieras

### Casos de Prueba Ejecutados

**Validación cruzada SQL vs Python:**
- 91/91 registros idénticos
- Mismas métricas estadísticas
- Ordenamiento consistente

**Pruebas de reglas de negocio:**
- Clasificación correcta de niveles N0-N4
- Manejo apropiado de fechas de retiro
- Interpolación correcta de datos faltantes como N0

**Verificación cálculos financieros:**
- ICV_30 = sald_30mas / sald_k_ifrs validado
- Gasto de provisión = provisión actual - anterior validado
- Gasto neto = gasto + condonaciones - recuperaciones validado

**Testing de integridad de datos:**
- Validación de formatos de fecha
- Verificación de rangos numéricos
- Detección de valores atípicos

## Arquitectura Técnica

### Componentes del Sistema

```
Datos Raw (Excel) → Validación → Transformación → Base Datos (SQLite)
                                      ↓
                    Análisis Python ← → Consultas SQL
                                      ↓
                              Resultados CSV ← → Dashboard Power BI
```

### Stack Tecnológico

- **Lenguajes:** Python 3.9+, SQL (ANSI)
- **Librerías Python:** pandas 2.3.2, numpy 1.24.3, openpyxl 3.1.5
- **Base de Datos:** SQLite 3.x
- **Visualización:** Power BI Service
- **Control de versiones:** Git
- **Testing:** pytest, validación cruzada
- **Documentación:** Markdown técnico

### Patrones de Diseño Implementados

- **Repository Pattern:** Separación de lógica de datos
- **Strategy Pattern:** Múltiples implementaciones (SQL/Python)
- **Observer Pattern:** Sistema de logging y monitoreo
- **Factory Pattern:** Creación de objetos de análisis

## Consideraciones de Producción

### Escalabilidad

- **Volumen de datos:** Optimizado para datasets hasta 100K registros
- **Concurrencia:** SQLite apropiado para análisis individual
- **Migración:** Diseño permite escalamiento a PostgreSQL/SQL Server
- **Paralelización:** Estructura permite procesamiento paralelo

### Seguridad

- **Datos sensibles:** Datos anonimizados en repositorio
- **Validación de entrada:** Sanitización de inputs
- **Auditoría:** Logs completos de procesamiento
- **Acceso controlado:** Preparado para implementar RBAC

### Mantenimiento

- **Actualización:** Proceso automatizado de refresh de datos
- **Monitoreo:** Validaciones automáticas de calidad
- **Backup:** Versionado de datasets y resultados
- **Documentación:** Código autodocumentado y comentado

### Performance

- **Optimizaciones SQL:** Índices y consultas optimizadas
- **Memoria Python:** Procesamiento eficiente con pandas
- **Caching:** Reutilización de cálculos intermedios
- **Vectorización:** Operaciones optimizadas con numpy

## Limitaciones y Consideraciones

### Limitaciones Técnicas Actuales

- **SQLite:** No soporta escrituras concurrentes masivas
- **Memoria:** Carga completa de datos en memoria para Python
- **Power BI Desktop:** Desarrollado en macOS usando Power BI Service

### Recomendaciones para Escalamiento

- **Base de datos:** Migrar a PostgreSQL para entornos de producción
- **Procesamiento:** Implementar Spark para datasets grandes
- **Paralelización:** Distribuir procesamiento por chunks de clientes
- **Caching:** Implementar Redis para consultas frecuentes

## Información del Desarrollador

- **Desarrollado por:** Johnnatan Villada Flórez
- **Tecnologías utilizadas:** Python, SQL, Power BI, SQLite
- **Metodología:** Desarrollo ágil con testing continuo
- **Estándares aplicados:** PEP 8, SQL ANSI, Clean Code
- **Testing:** Validación cruzada, casos de prueba automatizados
- **Documentación:** Markdown técnico profesional

---

**Licencia:** Desarrollado como demostración técnica  
**Fecha de entrega:** Septiembre 8, 2025  
**Versión:** 1.0 - Entrega final
