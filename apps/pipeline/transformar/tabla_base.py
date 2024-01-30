
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

    def transformar_df(self, ruta):
        df = self._procesar_csv(ruta)

        if df is not None:
            return self._limpiar_df(df)
        else:
            raise ValueError("Error al procesar el DataFrame")

