
# transformar/tabla_base.py
import pandas as pd
import os
from abc import ABC

class TablaBase(ABC):
    def __init__(self):
        pass
    
    def _procesar_csv(self, ruta):
        if os.path.exists(ruta):
            return pd.read_csv(ruta) 
        else:
            raise FileNotFoundError(f"La ruta no existe {ruta}")
    
    def _limpiar_df(self, df):
        df = df.map(lambda x: x.upper() if isinstance(x, str) else x)
    
        if 'Fecha' in df.columns:
            df = df.dropna(subset=['Fecha'])

        return df
    
    def aplicar_fechas(self, df):
        if 'Fecha' in df.columns:
            df['Fecha'] = pd.to_datetime(df['Fecha'])
            
            df['Dia_del_Año'] = df['Fecha'].dt.dayofyear
            df['Dia_de_la_Semana'] = df['Fecha'].dt.day_of_week
            df['Dia_del_Mes'] = df['Fecha'].dt.day
            df['Numero_de_Semana'] = df['Fecha'].dt.isocalendar().week
            df['Numero_de_Mes'] = df['Fecha'].dt.month
            df['Año'] = df['Fecha'].dt.year

            return df
        else:
            print('No se pudo aplicar el tratamiento de Fechas.')

    def transformar_df(self, ruta):
        df = self._procesar_csv(ruta)

        if df is not None:
            return self._limpiar_df(df)
        else:
            raise ValueError("Error al procesar el DataFrame")

