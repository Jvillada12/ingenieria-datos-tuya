#!/usr/bin/env python3
"""
Ejecutor de consulta SQL para análisis de rachas
Compara resultados SQL vs Python
"""

import pandas as pd
from src.ejercicio3_rachas.python.database_manager import DatabaseManager

def main():
    """Ejecuta análisis de rachas SQL y compara con Python"""
    
    print("EJECUTANDO ANÁLISIS DE RACHAS SQL")
    print("="*50)
    
    try:
        # Conectar a la base de datos
        db_manager = DatabaseManager()
        db_manager.connect()
        
        # Ejecutar consulta SQL de rachas
        print("Ejecutando consulta SQL...")
        resultado_sql = db_manager.execute_rachas_query(min_racha=3, fecha_base='2024-12-31')
        
        if not resultado_sql.empty:
            print(f"Resultado SQL: {len(resultado_sql)} clientes con rachas")
            
            # Exportar resultado SQL
            csv_sql = db_manager.export_results_to_csv('data/output/rachas_sql_resultado.csv')
            
            # Mostrar top 10 rachas SQL
            print(f"\nTOP 10 RACHAS (SQL):")
            for i, (_, row) in enumerate(resultado_sql.head(10).iterrows(), 1):
                print(f"  {i:2d}. {row['identificacion']}: {row['racha']} meses "
                      f"(Nivel {row['nivel']}, hasta {row['fecha_fin']})")
            
            # Comparar con resultado Python si existe
            try:
                resultado_python = pd.read_csv('data/output/rachas_resultado.csv')
                print(f"\nCOMPARACIÓN SQL vs PYTHON:")
                print(f"  SQL:    {len(resultado_sql)} clientes")
                print(f"  Python: {len(resultado_python)} clientes")
                
                if len(resultado_sql) == len(resultado_python):
                    print("  Misma cantidad de clientes")
                else:
                    print("  Diferencia en cantidad de clientes")
                
                # Comparar algunos casos específicos
                sql_sample = resultado_sql.head(5)[['identificacion', 'racha', 'nivel']].sort_values('identificacion')
                python_sample = resultado_python.head(5)[['identificacion', 'racha', 'nivel']].sort_values('identificacion')
                
                print(f"\nMUESTRA COMPARATIVA (primeros 5):")
                print("SQL:")
                print(sql_sample.to_string(index=False))
                print("\nPython:")
                print(python_sample.to_string(index=False))
                
            except FileNotFoundError:
                print("\nNo se encontró resultado Python para comparar")
            
            # Estadísticas del análisis SQL
            print(f"\nESTADÍSTICAS SQL:")
            print(f"  Racha promedio: {resultado_sql['racha'].mean():.1f} meses")
            print(f"  Racha máxima: {resultado_sql['racha'].max()} meses")
            print(f"  Racha mínima: {resultado_sql['racha'].min()} meses")
            
            # Distribución por niveles
            dist_niveles = resultado_sql['nivel'].value_counts().sort_index()
            print(f"\nDISTRIBUCIÓN POR NIVEL:")
            for nivel, cantidad in dist_niveles.items():
                porcentaje = (cantidad / len(resultado_sql)) * 100
                print(f"  {nivel}: {cantidad} clientes ({porcentaje:.1f}%)")
        
        else:
            print("No se encontraron rachas que cumplan los criterios")
        
        # Obtener estadísticas generales
        stats = db_manager.get_statistics()
        print(f"\nESTADÍSTICAS GENERALES:")
        print(f"  Registros originales: {stats['total_registros_historia']}")
        print(f"  Registros interpolados: {stats['registros_serie_completa']}")
        print(f"  Clientes únicos: {stats['clientes_unicos']}")
        print(f"  Clientes con rachas: {stats.get('clientes_con_rachas', 0)}")
        
        # Cerrar conexión
        db_manager.disconnect()
        
        print(f"\nANÁLISIS SQL COMPLETADO")
        print("="*50)
        
    except Exception as e:
        print(f"Error en análisis SQL: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
