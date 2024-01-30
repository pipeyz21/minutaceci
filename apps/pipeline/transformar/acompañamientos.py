import pandas as pd
import os
from ingresos import TablaIngresos
from dotenv import load_dotenv

class TablaAcompañamientos(TablaIngresos):
    def transformar_acompañamientos(self, ruta_almuerzos, ruta_otros):
        df = self.transformar_ingresos(ruta_almuerzos, ruta_otros)
        df = pd.DataFrame(df['Acompañamiento'].drop_duplicates()).reset_index()
        df = df.drop(columns='index').reset_index().rename(columns={'index':'id_acompañamiento'})

        return df
    
    def procesar_acompañamientos(self, ruta_almuerzos, ruta_otros):
        return self.transformar_acompañamientos(ruta_almuerzos, ruta_otros)
    
    def transformar_ingresos(self, ruta_almuerzos, ruta_otros):
        return super().transformar_ingresos(ruta_almuerzos, ruta_otros)
    
if __name__ == "__main__":

    load_dotenv()
    ruta_almuerzos = os.getenv('RUTA_ALMUERZOS')
    ruta_otros = os.getenv('RUTA_OTROS')
    instancia = TablaAcompañamientos()
    df_transformado = instancia.procesar_acompañamientos(ruta_almuerzos, ruta_otros)
    print(df_transformado)