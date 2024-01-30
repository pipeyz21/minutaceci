import pandas as pd
import os
from tabla_base import TablaBase
from dotenv import load_dotenv

class TablaProveedores(TablaBase):
    def transformar_df(self, ruta):
        return super().transformar_df(ruta)
    
    def procesar_proveedores(self, ruta):
        return self.transformar_df(ruta)
    
if __name__ == "__main__":

    load_dotenv()
    ruta_csv = os.getenv('RUTA_PROVEEDORES')
    instancia = TablaProveedores()
    df_transformado = instancia.transformar_df(ruta_csv)
    print(df_transformado)