#!/usr/bin/env python3
"""
Exploración inicial de los datos para el ejercicio de rachas
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def explorar_archivo_rachas():
    """Explora el archivo Rachas.xlsx"""
    print("="*50)
    print("EXPLORACIÓN ARCHIVO RACHAS.XLSX")
    print("="*50)
    
    # Cargar hoja historia
    print("\nHOJA: historia")
    print("-" * 30)
    historia_df = pd.read_excel('../../data/raw/Rachas.xlsx', sheet_name='historia')
    
    print(f"Dimensiones: {historia_df.shape}")
    print(f"Columnas: {list(historia_df.columns)}")
    print("\nTipos de datos:")
    print(historia_df.dtypes)
    
    print("\nPrimeras 5 filas:")
    print(historia_df.head())
    
    print("\nEstadísticas básicas:")
    print(historia_df.describe())
    
    # Análisis de fechas
    print(f"\nFechas únicas: {historia_df['corte_mes'].nunique()}")
    print(f"Rango de fechas: {historia_df['corte_mes'].min()} a {historia_df['corte_mes'].max()}")
    
    # Análisis de clientes
    print(f"\nClientes únicos: {historia_df['identificacion'].nunique()}")
    
    # Análisis de saldos por nivel
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
    
    print("\nDistribución por niveles:")
    print(historia_df['nivel'].value_counts().sort_index())
    
    # Cargar hoja retiros
    print("\nHOJA: retiros")
    print("-" * 30)
    retiros_df = pd.read_excel('../../data/raw/Rachas.xlsx', sheet_name='retiros')
    
    print(f"Dimensiones: {retiros_df.shape}")
    print(f"Columnas: {list(retiros_df.columns)}")
    
    print("\nTodos los retiros:")
    print(retiros_df)
    
    return historia_df, retiros_df

def explorar_archivo_provisiones():
    """Explora el archivo Provisiones.xlsx"""
    print("\n" + "="*50)
    print("EXPLORACIÓN ARCHIVO PROVISIONES.XLSX")
    print("="*50)
    
    # Cargar hoja Prov
    print("\nHOJA: Prov")
    print("-" * 30)
    prov_df = pd.read_excel('../../data/raw/Provisiones.xlsx', sheet_name='Prov')
    
    print(f"Dimensiones: {prov_df.shape}")
    print(f"Columnas: {list(prov_df.columns)}")
    print("\nTipos de datos:")
    print(prov_df.dtypes)
    
    print("\nPrimeras 5 filas:")
    print(prov_df.head())
    
    # Cargar hoja cond y recu
    print("\nHOJA: cond y recu")
    print("-" * 30)
    cond_recu_df = pd.read_excel('../../data/raw/Provisiones.xlsx', sheet_name='cond y recu')
    
    print(f"Dimensiones: {cond_recu_df.shape}")
    print(f"Columnas: {list(cond_recu_df.columns)}")
    
    print("\nPrimeras 5 filas:")
    print(cond_recu_df.head())
    
