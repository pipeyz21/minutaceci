import pandas as pd
import os
import numpy as np
from dotenv import load_dotenv
import matplotlib.pyplot as plt

def indicadores_ingresos(df, periodo_tiempo, fecha_inicio=None, fecha_fin=None):
    
    # Definir valores predeterminados para fecha_inicio y fecha_fin si no se proporcionan
    if fecha_inicio is None:
        fecha_inicio = df['Fecha'].min()
    if fecha_fin is None:
        fecha_fin = df['Fecha'].max()

    # Definir columnas a filtrar y a agrupar
    filtrar = [periodo_tiempo, 'Total', 'id_cliente', 'Boleta', 'Cantidad', 'id_producto']
    agrupar = [periodo_tiempo]

    # Filtrar el DataFrame
    df_filtrado = df[(df['Fecha'] >= fecha_inicio) & (df['Fecha'] <= fecha_fin)]
    
    # Realizar agrupación y cálculos de indicadores
    df_indicadores = df_filtrado[filtrar].groupby(agrupar).agg({
        'Total': 'sum',
        'id_cliente': 'nunique',
        'Boleta': 'nunique',
        'Cantidad': 'sum',
        'id_producto': 'nunique'
    })

    # Renombrar columnas
    df_indicadores = df_indicadores.rename(columns={'id_cliente': 'N°_Clientes', 'id_producto': 'N°_Productos'})

    # Calcular indicadores adicionales
    df_indicadores['Ticket_Promedio'] = df_indicadores['Total'] / df_indicadores['Boleta']
    df_indicadores['Productos_Boleta'] = df_indicadores['Cantidad'] / df_indicadores['Boleta']
    df_indicadores['Gasto_Cliente'] = df_indicadores['Total'] / df_indicadores['N°_Clientes']

    # Resetear el índice para obtener una estructura de DataFrame más plana
    return df_indicadores.reset_index().rename(columns={periodo_tiempo:'Periodo'})

class SMA:
    def __init__(self) -> None:
        pass

    def predecir(self, df, columna_a_predecir, periodos):
        print('Iniciando la predicción')
        fila = {'Periodo':df['Periodo'].max()+1, columna_a_predecir:np.nan}
        df = df._append(fila, ignore_index=True)

        df['SMA'] = df[columna_a_predecir].rolling(window=periodos).mean().shift(1)
        return df
    
    def graficar(self, df, columna_demanda, columna_pronostico):
        print('Graficando...')
        fig, ax = plt.subplots(figsize=(8, 6))

        ax.plot(df['Periodo'], df[columna_demanda], color='red', marker='o', label='Demanda')
        ax.plot(df['Periodo'], df[columna_pronostico], color='black', marker='o', label='Pronostico')

        ax.legend(loc = 'upper left')
        ax.set_title('Pronosticos por Periodo')
        ax.set_xlabel('Periodo')
        ax.set_ylabel('Demanda')

        plt.tight_layout()
        plt.show()

    def resultado(self, df, columna_a_predecir, periodos):
        df = self.predecir(df, columna_a_predecir, periodos)

        indice = df[df['Periodo']==df['Periodo'].max()].index.to_list()
        n = int(df.iloc[indice[0], 0])
        p = int(df.iloc[indice[0], 2])
        print(f"El prónostico para el periodo {n} es de {p:,} ")

        self.graficar(df, columna_a_predecir, 'SMA')

load_dotenv()


sma = SMA()
ruta_ingresos = os.getenv('RUTA_INGRESOS')
ingresos = pd.read_csv(ruta_ingresos)

ingresos = indicadores_ingresos(ingresos, 'Semana', '2023-01-01', '2023-12-31')[['Periodo','Total']]

sma.resultado(ingresos, 'Total', 3)
