#!/usr/bin/env python3
"""
Exploración rápida de datos desde la carpeta principal
"""

import pandas as pd
import numpy as np
from datetime import datetime

def main():
    print("INICIANDO EXPLORACIÓN DE DATOS")
    print("="*50)
    
    try:
        # Leer archivo de rachas
        print("Cargando Rachas.xlsx...")
        historia_df = pd.read_excel('data/raw/Rachas.xlsx', sheet_name='historia')
        retiros_df = pd.read_excel('data/raw/Rachas.xlsx', sheet_name='retiros')
        
        print(f"Historia cargada: {historia_df.shape} filas, {historia_df.shape[1]} columnas")
        print(f"Retiros cargados: {retiros_df.shape} filas, {retiros_df.shape[1]} columnas")
        
        # Mostrar información básica
        print("\nINFORMACIÓN HISTORIA:")
        print(f"Columnas: {list(historia_df.columns)}")
        print(f"Clientes únicos: {historia_df['identificacion'].nunique()}")
        print(f"Fechas únicas: {historia_df['corte_mes'].nunique()}")
        print(f"Rango fechas: {historia_df['corte_mes'].min()} a {historia_df['corte_mes'].max()}")
        
        print("\nPRIMERAS 5 FILAS HISTORIA:")
        print(historia_df.head())
        
        print("\nINFORMACIÓN RETIROS:")
        print(f"Columnas: {list(retiros_df.columns)}")
        print("\nTODOS LOS RETIROS:")
        print(retiros_df)
        
        # Clasificar por niveles
        def clasificar_nivel(saldo):
            if saldo >= 0 and saldo < 300000:
                return 'N0'
            elif saldo >= 300000 and saldo < 1000000:
                return 'N1'
            elif saldo >= 1000000 and saldo < 3000000:
                return 'N2'
            elif saldo >= 3000000 and saldo < 5000000:
                return 'N3'
            elif saldo >= 5000000:
                return 'N4'
            else:
                return 'ERROR'
        
        historia_df['nivel'] = historia_df['saldo'].apply(clasificar_nivel)
        
        print("\nDISTRIBUCIÓN POR NIVELES:")
        distribucion = historia_df['nivel'].value_counts().sort_index()
        for nivel, cantidad in distribucion.items():
            porcentaje = (cantidad / len(historia_df)) * 100
            print(f"  {nivel}: {cantidad:,} registros ({porcentaje:.1f}%)")
        
        # Análisis por cliente
        print("\nANÁLISIS POR CLIENTE:")
        registros_por_cliente = historia_df.groupby('identificacion').size().describe()
        print(f"Estadísticas de registros por cliente:")
        print(f"  Promedio: {registros_por_cliente['mean']:.1f}")
        print(f"  Mínimo: {int(registros_por_cliente['min'])}")
        print(f"  Máximo: {int(registros_por_cliente['max'])}")
        
        # Top 5 clientes con más registros
        top_clientes = historia_df['identificacion'].value_counts().head()
        print(f"\nTOP 5 CLIENTES CON MÁS REGISTROS:")
        for i, (cliente, registros) in enumerate(top_clientes.items(), 1):
            print(f"  {i}. {cliente}: {registros} registros")
        
        # Análisis temporal
        print("\nANÁLISIS TEMPORAL:")
        fechas_unicas = sorted(historia_df['corte_mes'].unique())
        print(f"Total de fechas únicas: {len(fechas_unicas)}")
        print(f"Primera fecha: {fechas_unicas[0]}")
        print(f"Última fecha: {fechas_unicas[-1]}")
        
        print("\nEXPLORACIÓN COMPLETADA EXITOSAMENTE!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
