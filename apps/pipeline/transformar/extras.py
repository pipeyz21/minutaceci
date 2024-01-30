import pandas as pd
import os
from ingresos import TablaIngresos
from dotenv import load_dotenv

class TablaExtras(TablaIngresos):
    def transformar_extras(self, ruta_almuerzos, ruta_otros):
        df = self.transformar_ingresos(ruta_almuerzos, ruta_otros)
        df = pd.DataFrame(df['Ensalada'].drop_duplicates()).reset_index()
        df = df.drop(columns='index').reset_index().rename(columns={'index':'id_extra'})
        return df
    
    def procesar_extras(self, ruta_almuerzos, ruta_otros):
        return self.transformar_extras(ruta_almuerzos, ruta_otros)
    

    
if __name__ == "__main__":

    load_dotenv()

    ruta_almuerzos = os.getenv('RUTA_ALMUERZOS')
    ruta_otros = os.getenv('RUTA_OTROS')

    instancia = TablaExtras()
    df_transformado = instancia.procesar_extras(ruta_almuerzos, ruta_otros)
    print(df_transformado)