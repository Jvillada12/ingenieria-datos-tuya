#!/usr/bin/env python3
"""
Database Manager para el análisis de rachas
Maneja la conexión SQLite y carga de datos desde Excel
"""

import sqlite3
import pandas as pd
import os
from pathlib import Path

class DatabaseManager:
    """Manager para base de datos SQLite del análisis de rachas"""
    
    def __init__(self, db_path='data/output/rachas.db'):
        """
        Inicializa el manager de base de datos
        
        Args:
            db_path (str): Ruta al archivo de base de datos SQLite
        """
        self.db_path = db_path
        self.conn = None
        
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    def connect(self):
        """Establece conexión con la base de datos"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")  # Habilitar foreign keys
            print(f"Conexión establecida con: {self.db_path}")
            return self.conn
        except Exception as e:
            print(f"Error conectando a la base de datos: {e}")
            raise
    
    def disconnect(self):
        """Cierra la conexión con la base de datos"""
        if self.conn:
            self.conn.close()
            print("Conexión cerrada")
    
    def create_schema(self, schema_file='src/ejercicio3_rachas/sql/schema.sql'):
        """
        Crea el esquema de base de datos desde el archivo SQL
        
        Args:
            schema_file (str): Ruta al archivo schema.sql
        """
        print(f"Creando esquema desde: {schema_file}")
        
        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Ejecutar cada statement del schema
            cursor = self.conn.cursor()
            cursor.executescript(schema_sql)
            self.conn.commit()
            
            print("Esquema creado exitosamente")
            
            # Verificar tablas creadas
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            print(f"Tablas creadas: {[table[0] for table in tables]}")
            
        except Exception as e:
            print(f"Error creando esquema: {e}")
            raise
    
    def load_data_from_excel(self, excel_file='data/raw/Rachas.xlsx'):
        """
        Carga datos desde archivo Excel a las tablas
        
        Args:
            excel_file (str): Ruta al archivo Excel
        """
        print(f"Cargando datos desde: {excel_file}")
        
        try:
            # Cargar hoja historia
            print("Cargando datos de historia...")
            historia_df = pd.read_excel(excel_file, sheet_name='historia')
            historia_df['corte_mes'] = pd.to_datetime(historia_df['corte_mes'])
            
            # Insertar en tabla historia
            historia_df.to_sql('historia', self.conn, if_exists='append', index=False)
            print(f"Historia cargada: {len(historia_df)} registros")
            
            # Cargar hoja retiros
            print("Cargando datos de retiros...")
            retiros_df = pd.read_excel(excel_file, sheet_name='retiros')
            retiros_df['fecha_retiro'] = pd.to_datetime(retiros_df['fecha_retiro'])
            
            # Insertar en tabla retiros
            retiros_df.to_sql('retiros', self.conn, if_exists='append', index=False)
            print(f"Retiros cargados: {len(retiros_df)} registros")
            
            self.conn.commit()
            print("Datos guardados exitosamente")
            
        except Exception as e:
            print(f"Error cargando datos: {e}")
            self.conn.rollback()
            raise
    
    def generate_complete_series(self, fecha_base='2024-12-31'):
        """
        Genera la serie temporal completa con interpolación de datos faltantes
        """
        print(f"Generando serie temporal completa hasta: {fecha_base}")
        
        try:
            # Query simplificada para SQLite
            query = f"""
            WITH fechas_unicas AS (
                SELECT DISTINCT corte_mes 
                FROM historia 
                WHERE corte_mes <= '{fecha_base}'
            ),
            clientes_unicos AS (
                SELECT DISTINCT identificacion,
                    MIN(corte_mes) as primera_fecha
                FROM historia 
                GROUP BY identificacion
            ),
            serie_base AS (
                SELECT 
                    c.identificacion,
                    f.corte_mes,
                    COALESCE(h.saldo, 0) as saldo,
                    CASE WHEN h.saldo IS NOT NULL THEN 1 ELSE 0 END as es_real
                FROM clientes_unicos c
                CROSS JOIN fechas_unicas f
                LEFT JOIN historia h ON c.identificacion = h.identificacion 
                                    AND f.corte_mes = h.corte_mes
                LEFT JOIN retiros r ON c.identificacion = r.identificacion
                WHERE f.corte_mes >= c.primera_fecha
                AND (r.fecha_retiro IS NULL OR f.corte_mes <= r.fecha_retiro)
            )
            INSERT INTO historia_completa (identificacion, corte_mes, saldo, nivel, es_real)
            SELECT 
                identificacion,
                corte_mes,
                saldo,
                CASE 
                    WHEN saldo >= 0 AND saldo < 300000 THEN 'N0'
                    WHEN saldo >= 300000 AND saldo < 1000000 THEN 'N1'
                    WHEN saldo >= 1000000 AND saldo < 3000000 THEN 'N2'
                    WHEN saldo >= 3000000 AND saldo < 5000000 THEN 'N3'
                    WHEN saldo >= 5000000 THEN 'N4'
                    ELSE 'ERROR'
                END as nivel,
                es_real
            FROM serie_base
            """
            
            # Limpiar tabla primero
            self.conn.execute("DELETE FROM historia_completa")
            
            # Ejecutar query
            self.conn.execute(query)
            self.conn.commit()
            
            # Verificar resultados
            cursor = self.conn.execute("SELECT COUNT(*) FROM historia_completa")
            total = cursor.fetchone()[0]
            
            print(f"Serie temporal completa generada: {total} registros")
            
        except Exception as e:
            print(f"Error generando serie completa: {e}")
            self.conn.rollback()
            raise
    
    def execute_rachas_query(self, min_racha=3, fecha_base='2024-12-31'):
        """
        Ejecuta la consulta de rachas desde archivo SQL
        
        Args:
            min_racha (int): Mínimo de meses consecutivos
            fecha_base (str): Fecha base para el análisis
            
        Returns:
            pd.DataFrame: Resultado del análisis de rachas
        """
        print(f"Ejecutando consulta de rachas (min: {min_racha}, fecha: {fecha_base})")
        
        try:
            # Leer consulta desde archivo
            query_file = 'src/ejercicio3_rachas/sql/rachas_query.sql'
            with open(query_file, 'r', encoding='utf-8') as f:
                rachas_query = f.read()
            
            # Reemplazar parámetros en la consulta
            rachas_query = rachas_query.replace('2024-12-31', fecha_base)
            rachas_query = rachas_query.replace('COUNT(*) >= 3', f'COUNT(*) >= {min_racha}')
            
            # Ejecutar consulta
            resultado_df = pd.read_sql_query(rachas_query, self.conn)
            
            print(f"Consulta ejecutada: {len(resultado_df)} clientes con rachas válidas")
            
            # Guardar resultado en tabla
            if not resultado_df.empty:
                resultado_df['fecha_base'] = fecha_base
                resultado_df['min_racha'] = min_racha
                
                # Limpiar tabla de resultados
                cursor = self.conn.cursor()
                cursor.execute("DELETE FROM rachas_resultado WHERE fecha_base = ? AND min_racha = ?", 
                              (fecha_base, min_racha))
                
                # Insertar nuevos resultados
                resultado_df.to_sql('rachas_resultado', self.conn, if_exists='append', index=False)
                self.conn.commit()
            
            return resultado_df
            
        except Exception as e:
            print(f"Error ejecutando consulta de rachas: {e}")
            raise
    
    def export_results_to_csv(self, output_file='data/output/rachas_sql_resultado.csv'):
        """
        Exporta los resultados a CSV
        
        Args:
            output_file (str): Archivo de salida
        """
        try:
            query = """
            SELECT identificacion, racha, fecha_fin, nivel
            FROM rachas_resultado 
            ORDER BY racha DESC, fecha_fin DESC
            """
            
            df = pd.read_sql_query(query, self.conn)
            df.to_csv(output_file, index=False)
            
            print(f"Resultados exportados a: {output_file}")
            return df
            
        except Exception as e:
            print(f"Error exportando resultados: {e}")
            raise
    
    def get_statistics(self):
        """Obtiene estadísticas del análisis"""
        try:
            stats = {}
            
            # Estadísticas básicas
            cursor = self.conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM historia")
            stats['total_registros_historia'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT identificacion) FROM historia")
            stats['clientes_unicos'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM historia_completa")
            stats['registros_serie_completa'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM rachas_resultado")
            stats['clientes_con_rachas'] = cursor.fetchone()[0]
            
            # Distribución por niveles
            cursor.execute("""
                SELECT nivel, COUNT(*) as cantidad
                FROM rachas_resultado 
                GROUP BY nivel 
                ORDER BY nivel
            """)
            stats['distribucion_niveles'] = dict(cursor.fetchall())
            
            # Estadísticas de rachas
            cursor.execute("""
                SELECT 
                    AVG(racha) as racha_promedio,
                    MIN(racha) as racha_minima,
                    MAX(racha) as racha_maxima
                FROM rachas_resultado
            """)
            racha_stats = cursor.fetchone()
            stats['racha_promedio'] = racha_stats[0]
            stats['racha_minima'] = racha_stats[1]
            stats['racha_maxima'] = racha_stats[2]
            
            return stats
            
        except Exception as e:
            print(f"Error obteniendo estadísticas: {e}")
            raise

def main():
    """Función principal de prueba"""
    try:
        # Crear instancia del manager
        db_manager = DatabaseManager()
        
        # Conectar a la base de datos
        db_manager.connect()
        
        # Crear esquema
        db_manager.create_schema()
        
        # Cargar datos desde Excel
        db_manager.load_data_from_excel()
        
        # Generar serie temporal completa
        db_manager.generate_complete_series()
        
        print("\n" + "="*50)
        print("BASE DE DATOS CONFIGURADA EXITOSAMENTE")
        print("="*50)
        
        # Obtener estadísticas
        stats = db_manager.get_statistics()
        print(f"Registros en historia: {stats['total_registros_historia']}")
        print(f"Clientes únicos: {stats['clientes_unicos']}")
        print(f"Registros serie completa: {stats['registros_serie_completa']}")
        
        # Cerrar conexión
        db_manager.disconnect()
        
    except Exception as e:
        print(f"Error en la configuración: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()