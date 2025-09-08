#!/usr/bin/env python3
"""
Preparación de datos para dashboard Power BI
Análisis de provisiones con cálculo de métricas requeridas
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

class PowerBIDataPreparator:
    """Preparador de datos para dashboard Power BI de provisiones"""
    
    def __init__(self):
        """Inicializa el preparador de datos"""
        self.prov_df = None
        self.cond_recu_df = None
        self.dataset_final = None
    
    def load_provisiones_data(self, archivo='data/raw/Provisiones.xlsx'):
        """Carga datos desde archivo Excel de provisiones"""
        print(f"Cargando datos de provisiones desde: {archivo}")
        
        try:
            self.prov_df = pd.read_excel(archivo, sheet_name='Prov')
            print(f"Datos de provisiones cargados: {len(self.prov_df)} registros")
            
            self.cond_recu_df = pd.read_excel(archivo, sheet_name='cond y recu')
            print(f"Datos de condonaciones y recuperaciones cargados: {len(self.cond_recu_df)} registros")
            
            self._validate_data_quality()
            
        except Exception as e:
            print(f"Error cargando datos: {e}")
            raise
    
    def _validate_data_quality(self):
        """Valida la calidad e integridad de los datos cargados"""
        print("Validando calidad de datos...")
        
        prov_completeness = (self.prov_df.notna().sum() / len(self.prov_df)) * 100
        cond_completeness = (self.cond_recu_df.notna().sum() / len(self.cond_recu_df)) * 100
        
        print("Completitud de datos (%):")
        print("Provisiones:")
        for col, pct in prov_completeness.items():
            print(f"  {col}: {pct:.1f}%")
        
        print("Condonaciones y Recuperaciones:")
        for col, pct in cond_completeness.items():
            print(f"  {col}: {pct:.1f}%")
        
        prov_duplicates = self.prov_df.duplicated().sum()
        cond_duplicates = self.cond_recu_df.duplicated().sum()
        
        print(f"Registros duplicados - Provisiones: {prov_duplicates}, Cond/Recu: {cond_duplicates}")
        
        if 'fecha_analisis' in self.prov_df.columns:
            fecha_min = self.prov_df['fecha_analisis'].min()
            fecha_max = self.prov_df['fecha_analisis'].max()
            print(f"Rango de fechas provisiones: {fecha_min} a {fecha_max}")
    
    def calculate_metrics(self):
        """Calcula las métricas requeridas para el dashboard"""
        print("Calculando métricas financieras...")
        
        try:
            dataset = self.prov_df.copy()
            
            # Convertir fecha_analisis de formato YYYYMM a datetime
            dataset['fecha_analisis'] = pd.to_datetime(
                dataset['fecha_analisis'].astype(str), 
                format='%Y%m'
            )
            
            # 1. Calcular ICV_30
            dataset['icv_30'] = np.where(
                dataset['sald_k_ifrs'] != 0,
                dataset['sald_30mas'] / dataset['sald_k_ifrs'],
                0
            )
            
            # 2. Calcular Gasto de Provisión
            dataset = dataset.sort_values(['alianza', 'producto', 'fecha_analisis'])
            dataset['prov_mes_anterior'] = dataset.groupby(['alianza', 'producto'])['prov_k_ifrs'].shift(1)
            dataset['gasto_provision'] = dataset['prov_k_ifrs'] - dataset['prov_mes_anterior'].fillna(0)
            
            # 3. Preparar datos de condonaciones y recuperaciones
            cond_recu_work = self.cond_recu_df.copy()
            
            # Convertir fecha_analisis en cond_recu_df (ya está en datetime)
            if cond_recu_work['fecha_analisis'].dtype != 'datetime64[ns]':
            # Si es numérico, convertir desde serial de Excel
                cond_recu_work['fecha_analisis'] = pd.to_datetime(
                cond_recu_work['fecha_analisis'], 
                origin='1899-12-30', 
                unit='D'
        )
# Si ya es datetime, no hacer nada
            
            # Normalizar fechas al primer día del mes
            cond_recu_work['fecha_analisis'] = cond_recu_work['fecha_analisis'].dt.to_period('M').dt.start_time
            dataset['fecha_analisis'] = dataset['fecha_analisis'].dt.to_period('M').dt.start_time
            
            # Agregar condonaciones y recuperaciones
            cond_recu_agg = cond_recu_work.groupby(
                ['fecha_analisis', 'alianza', 'producto']
            ).agg({
                'condonaciones': 'sum',
                'recuperaciones': 'sum'
            }).reset_index()
            
            # Merge
            dataset = dataset.merge(
                cond_recu_agg,
                on=['fecha_analisis', 'alianza', 'producto'],
                how='left'
            )
            
            dataset['condonaciones'] = dataset['condonaciones'].fillna(0)
            dataset['recuperaciones'] = dataset['recuperaciones'].fillna(0)
            
            # 4. Calcular Gasto de Provisión Neto
            dataset['gasto_provision_neto'] = (
                dataset['gasto_provision'] + 
                dataset['condonaciones'] - 
                dataset['recuperaciones']
            )
            
            # 5. Métricas adicionales
            dataset['ratio_provision_saldo'] = np.where(
                dataset['sald_k_ifrs'] != 0,
                dataset['prov_k_ifrs'] / dataset['sald_k_ifrs'],
                0
            )
            
            dataset['cobertura_provision'] = np.where(
                dataset['sald_30mas'] != 0,
                dataset['prov_k_ifrs'] / dataset['sald_30mas'],
                0
            )
            
            # Dimensiones temporales
            dataset['año'] = dataset['fecha_analisis'].dt.year
            dataset['mes'] = dataset['fecha_analisis'].dt.month
            dataset['año_mes'] = dataset['fecha_analisis'].dt.strftime('%Y-%m')
            dataset['trimestre'] = dataset['fecha_analisis'].dt.quarter
            dataset['año_trimestre'] = dataset['año'].astype(str) + '-Q' + dataset['trimestre'].astype(str)
            
            self.dataset_final = dataset
            
            print("Métricas calculadas exitosamente:")
            print(f"  - ICV_30: Promedio {dataset['icv_30'].mean():.3f}")
            print(f"  - Gasto Provisión: Total {dataset['gasto_provision'].sum():,.0f}")
            print(f"  - Gasto Provisión Neto: Total {dataset['gasto_provision_neto'].sum():,.0f}")
            print(f"  - Registros procesados: {len(dataset)}")
            
        except Exception as e:
            print(f"Error calculando métricas: {e}")
            raise
    
    def export_for_powerbi(self, output_file='data/output/provisiones_powerbi.xlsx'):
        """Exporta datos preparados para Power BI"""
        if self.dataset_final is None:
            print("Error: Dataset final no disponible.")
            return None
        
        try:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            export_data = self.dataset_final.copy()
            
            columns_powerbi = [
                'fecha_analisis', 'año', 'mes', 'año_mes', 'trimestre', 'año_trimestre',
                'alianza', 'producto',
                'prov_k_ifrs', 'prov_t_ifrs',
                'sald_30mas', 'sald_k_ifrs', 'sald_t_ifrs',
                'saldo_castigo', 'saldo_castigo_t',
                'condonaciones', 'recuperaciones',
                'icv_30', 'gasto_provision', 'gasto_provision_neto',
                'ratio_provision_saldo', 'cobertura_provision'
            ]
            
            export_data = export_data[columns_powerbi]
            
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                export_data.to_excel(writer, sheet_name='Datos_Principales', index=False)
                
                resumen_alianza = export_data.groupby(['alianza']).agg({
                    'sald_k_ifrs': 'sum',
                    'prov_k_ifrs': 'sum',
                    'icv_30': 'mean',
                    'gasto_provision_neto': 'sum'
                }).round(2)
                resumen_alianza.to_excel(writer, sheet_name='Resumen_Alianza')
                
                resumen_producto = export_data.groupby(['producto']).agg({
                    'sald_k_ifrs': 'sum',
                    'prov_k_ifrs': 'sum',
                    'icv_30': 'mean',
                    'gasto_provision_neto': 'sum'
                }).round(2)
                resumen_producto.to_excel(writer, sheet_name='Resumen_Producto')
            
            print(f"Datos exportados exitosamente a: {output_file}")
            print(f"Registros exportados: {len(export_data)}")
            
            return export_data
            
        except Exception as e:
            print(f"Error exportando datos: {e}")
            raise

def main():
    """Función principal de preparación de datos"""
    try:
        print("Iniciando preparación de datos para Power BI")
        print("=" * 50)
        
        preparador = PowerBIDataPreparator()
        preparador.load_provisiones_data()
        preparador.calculate_metrics()
        preparador.export_for_powerbi()
        
        print("\nPreparación de datos completada exitosamente")
        print("Archivo generado: data/output/provisiones_powerbi.xlsx")
        
    except Exception as e:
        print(f"Error en preparación de datos: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()