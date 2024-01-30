import pandas as pd
import os
from tabla_base import TablaBase
from dotenv import load_dotenv

class TablaInsumos(TablaBase):
    def transformar_df(self, ruta):
        df = super().transformar_df(ruta)
        return df
    
    def procesar_insumos(self, ruta):
        return self.transformar_df(ruta)


if __name__ == "__main__":

    load_dotenv()
    ruta_csv = os.getenv('RUTA_MATERIAL')
    instancia = TablaInsumos()
    df_transformado = instancia.transformar_df(ruta_csv)
    print(df_transformado)