# transformar/tabla1.py
import os
from tabla_base import TablaBase
from dotenv import load_dotenv

class TablaArriendos(TablaBase):
    def transformar_df(self, ruta):
        df = super().transformar_df(ruta)

        df = df[['Fecha','Subsidio','Copago','Contrato2']]
        return df
    
    def procesar_arriendos(self, ruta):
        return self.transformar_df(ruta)
    
if __name__ == "__main__":

    load_dotenv()
    ruta_csv = os.getenv('RUTA_SUBSIDIO')
    instancia = TablaArriendos()
    df_transformado = instancia.procesar_arriendos(ruta_csv)
    print(df_transformado)
