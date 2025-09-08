#!/usr/bin/env python3
"""
Calculador de Rachas - Ejercicio 3
Identifica rachas consecutivas de clientes por nivel de deuda
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class CalculadorRachas:
    """Clase para calcular rachas de clientes por nivel de saldo"""
    
    def __init__(self, fecha_base='2024-12-31'):
        """
        Inicializa el calculador
        
        Args:
            fecha_base (str): Fecha base para el análisis (formato YYYY-MM-DD)
        """
        self.fecha_base = pd.to_datetime(fecha_base)
        self.historia_df = None
        self.retiros_df = None
        self.niveles_definidos = {
            'N0': (0, 300000),
            'N1': (300000, 1000000),
            'N2': (1000000, 3000000),
            'N3': (3000000, 5000000),
            'N4': (5000000, float('inf'))
        }
    
    def cargar_datos(self, archivo_rachas='data/raw/Rachas.xlsx'):
        """Carga los datos desde el archivo Excel"""
        print(f"Cargando datos desde {archivo_rachas}...")
        
        # Cargar historia
        self.historia_df = pd.read_excel(archivo_rachas, sheet_name='historia')
        self.historia_df['corte_mes'] = pd.to_datetime(self.historia_df['corte_mes'])
        
        # Cargar retiros
        self.retiros_df = pd.read_excel(archivo_rachas, sheet_name='retiros')
        self.retiros_df['fecha_retiro'] = pd.to_datetime(self.retiros_df['fecha_retiro'])
        
        print(f"Datos cargados: {len(self.historia_df)} registros de historia, {len(self.retiros_df)} retiros")
        
        # Filtrar por fecha base
        self.historia_df = self.historia_df[self.historia_df['corte_mes'] <= self.fecha_base]
        print(f"Filtrado por fecha base {self.fecha_base}: {len(self.historia_df)} registros")
    
    def clasificar_nivel(self, saldo):
        """Clasifica un saldo en su nivel correspondiente"""
        for nivel, (min_val, max_val) in self.niveles_definidos.items():
            if min_val <= saldo < max_val:
                return nivel
        return 'ERROR'
    
    def generar_serie_temporal_completa(self):
        """
        Genera una serie temporal completa para cada cliente
        Si un cliente no aparece en un mes, se asume N0 (excepto si se retiró)
        """
        print("Generando serie temporal completa...")
        
        # Obtener todas las fechas únicas
        fechas_unicas = sorted(self.historia_df['corte_mes'].unique())
        clientes_unicos = self.historia_df['identificacion'].unique()
        
        # Crear diccionario de retiros para consulta rápida
        retiros_dict = {}
        for _, row in self.retiros_df.iterrows():
            retiros_dict[row['identificacion']] = row['fecha_retiro']
        
        # Lista para almacenar la serie completa
        serie_completa = []
        
        for cliente in clientes_unicos:
            # Obtener datos del cliente
            datos_cliente = self.historia_df[
                self.historia_df['identificacion'] == cliente
            ].sort_values('corte_mes')
            
            # Fecha de retiro del cliente (si existe)
            fecha_retiro = retiros_dict.get(cliente)
            
            # Primera aparición del cliente
            primera_fecha = datos_cliente['corte_mes'].min()
            
            for fecha in fechas_unicas:
                # Solo considerar fechas desde la primera aparición
                if fecha < primera_fecha:
                    continue
                
                # Si el cliente se retiró y la fecha es posterior, no incluir
                if fecha_retiro and fecha > fecha_retiro:
                    continue
                
                # Buscar el registro para esta fecha
                registro = datos_cliente[datos_cliente['corte_mes'] == fecha]
                
                if len(registro) > 0:
                    # Existe registro real
                    saldo = registro.iloc[0]['saldo']
                    nivel = self.clasificar_nivel(saldo)
                else:
                    # No existe registro, asignar N0
                    saldo = 0
                    nivel = 'N0'
                
                serie_completa.append({
                    'identificacion': cliente,
                    'corte_mes': fecha,
                    'saldo': saldo,
                    'nivel': nivel,
                    'es_real': len(registro) > 0
                })
        
        # Convertir a DataFrame
        self.serie_completa_df = pd.DataFrame(serie_completa)
        self.serie_completa_df = self.serie_completa_df.sort_values(['identificacion', 'corte_mes'])
        
        print(f"Serie temporal completa generada: {len(self.serie_completa_df)} registros")
        return self.serie_completa_df
    
    def calcular_rachas(self, min_racha=1):
        """
        Calcula las rachas consecutivas por cliente y nivel
        
        Args:
            min_racha (int): Número mínimo de meses consecutivos para considerar una racha
            
        Returns:
            pd.DataFrame: DataFrame con las rachas calculadas
        """
        print(f"Calculando rachas (mínimo {min_racha} meses)...")
        
        if self.serie_completa_df is None:
            self.generar_serie_temporal_completa()
        
        rachas_resultado = []
        
        for cliente in self.serie_completa_df['identificacion'].unique():
            datos_cliente = self.serie_completa_df[
                self.serie_completa_df['identificacion'] == cliente
            ].sort_values('corte_mes')
            
            # Calcular rachas por nivel
            rachas_cliente = self._calcular_rachas_cliente(datos_cliente, min_racha)
            
            # Seleccionar la mejor racha según criterios
            mejor_racha = self._seleccionar_mejor_racha(rachas_cliente)
            
            if mejor_racha:
                rachas_resultado.append({
                    'identificacion': cliente,
                    'racha': mejor_racha['longitud'],
                    'fecha_fin': mejor_racha['fecha_fin'],
                    'nivel': mejor_racha['nivel']
                })
        
        # Convertir a DataFrame y ordenar
        resultado_df = pd.DataFrame(rachas_resultado)
        if not resultado_df.empty:
            resultado_df = resultado_df.sort_values(['racha', 'fecha_fin'], ascending=[False, False])
        
        print(f"Rachas calculadas: {len(resultado_df)} clientes con rachas >= {min_racha}")
        return resultado_df
    
    def _calcular_rachas_cliente(self, datos_cliente, min_racha):
        """Calcula todas las rachas de un cliente específico"""
        rachas = []
        
        for nivel in self.niveles_definidos.keys():
            # Crear serie booleana para este nivel
            es_nivel = (datos_cliente['nivel'] == nivel).values
            
            # Encontrar rachas consecutivas
            rachas_nivel = self._encontrar_rachas_consecutivas(
                es_nivel, datos_cliente['corte_mes'].values, nivel, min_racha
            )
            
            rachas.extend(rachas_nivel)
        
        return rachas
    
    def _encontrar_rachas_consecutivas(self, serie_bool, fechas, nivel, min_racha):
        """Encuentra rachas consecutivas en una serie booleana"""
        rachas = []
        racha_actual = 0
        inicio_racha = None
        
        for i, es_verdadero in enumerate(serie_bool):
            if es_verdadero:
                if racha_actual == 0:
                    inicio_racha = fechas[i]
                racha_actual += 1
            else:
                if racha_actual >= min_racha:
                    rachas.append({
                        'nivel': nivel,
                        'longitud': racha_actual,
                        'fecha_inicio': inicio_racha,
                        'fecha_fin': fechas[i-1]
                    })
                racha_actual = 0
        
        # Verificar si la última secuencia es una racha válida
        if racha_actual >= min_racha:
            rachas.append({
                'nivel': nivel,
                'longitud': racha_actual,
                'fecha_inicio': inicio_racha,
                'fecha_fin': fechas[-1]
            })
        
        return rachas
    
    def _seleccionar_mejor_racha(self, rachas_cliente):
        """
        Selecciona la mejor racha según los criterios:
        1. Racha más larga
        2. Si hay empate, la más reciente (fecha_fin más próxima a fecha_base)
        """
        if not rachas_cliente:
            return None
        
        # Filtrar solo rachas que terminan <= fecha_base
        rachas_validas = [
            racha for racha in rachas_cliente 
            if pd.to_datetime(racha['fecha_fin']) <= self.fecha_base
        ]
        
        if not rachas_validas:
            return None
        
        # Ordenar por longitud (desc) y luego por fecha_fin (desc)
        rachas_validas.sort(
            key=lambda x: (x['longitud'], pd.to_datetime(x['fecha_fin'])), 
            reverse=True
        )
        
        return rachas_validas[0]
    
    def generar_reporte(self, resultado_df):
        """Genera un reporte detallado de los resultados"""
        print("\n" + "="*60)
        print("REPORTE DE RACHAS")
        print("="*60)
        
        if resultado_df.empty:
            print("No se encontraron rachas que cumplan los criterios")
            return
        
        print(f"Total de clientes con rachas válidas: {len(resultado_df)}")
        print(f"Fecha base del análisis: {self.fecha_base.strftime('%Y-%m-%d')}")
        
        # Estadísticas generales
        print(f"\nESTADÍSTICAS GENERALES:")
        print(f"  Racha promedio: {resultado_df['racha'].mean():.1f} meses")
        print(f"  Racha más larga: {resultado_df['racha'].max()} meses")
        print(f"  Racha más corta: {resultado_df['racha'].min()} meses")
        
        # Distribución por nivel
        print(f"\nDISTRIBUCIÓN POR NIVEL:")
        dist_nivel = resultado_df['nivel'].value_counts().sort_index()
        for nivel, cantidad in dist_nivel.items():
            porcentaje = (cantidad / len(resultado_df)) * 100
            print(f"  {nivel}: {cantidad} clientes ({porcentaje:.1f}%)")
        
        # Top 10 rachas más largas
        print(f"\nTOP 10 RACHAS MÁS LARGAS:")
        top_rachas = resultado_df.head(10)
        for i, (_, row) in enumerate(top_rachas.iterrows(), 1):
            print(f"  {i:2d}. {row['identificacion']}: {row['racha']} meses "
                  f"(Nivel {row['nivel']}, hasta {row['fecha_fin'].strftime('%Y-%m-%d')})")
        
        return resultado_df

def main():
    """Función principal de prueba"""
    try:
        # Crear instancia del calculador
        calculador = CalculadorRachas(fecha_base='2024-12-31')
        
        # Cargar datos
        calculador.cargar_datos()
        
        # Generar serie temporal completa
        serie_completa = calculador.generar_serie_temporal_completa()
        
        # Calcular rachas con mínimo 3 meses
        resultado = calculador.calcular_rachas(min_racha=3)
        
        # Generar reporte
        calculador.generar_reporte(resultado)
        
        # Guardar resultado
        if not resultado.empty:
            resultado.to_csv('data/output/rachas_resultado.csv', index=False)
            print(f"\nResultado guardado en: data/output/rachas_resultado.csv")
        
        return resultado
        
    except Exception as e:
        print(f"Error en el cálculo de rachas: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
