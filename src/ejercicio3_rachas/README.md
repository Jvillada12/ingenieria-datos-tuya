# Ejercicio 3: Análisis de Rachas - Implementación Dual

**Desarrollado por:** Johnnatan Villada Flórez

## Descripción

Análisis de rachas consecutivas de clientes por nivel de deuda, implementando la lógica tanto en SQL como en Python para validación cruzada de resultados.

## Archivos Principales

- `sql/schema.sql` - Esquema de base de datos SQLite
- `sql/rachas_query.sql` - Consulta principal de rachas
- `python/database_manager.py` - Gestión de base de datos
- `python/explorar_datos.py` - Análisis exploratorio
- `../../calculador_rachas.py` - Script principal Python
- `../../ejecutar_rachas_sql.py` - Ejecutor consultas SQL

## Resultados

- 91 clientes con rachas válidas >= 3 meses
- 100% concordancia entre implementaciones SQL y Python
- Datos almacenados en `../../data/output/`

## Ejecución

```bash
# Python
python3 calculador_rachas.py

# SQL  
python3 ejecutar_rachas_sql.py