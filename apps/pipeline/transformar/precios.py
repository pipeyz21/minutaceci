import pandas as pd
import os
from productos import TablaProductos
from dotenv import load_dotenv

class TablaPrecios(TablaProductos):
    def unir_tablas(self, ruta):
        precios = self.transformar_df(ruta)
        productos = self.transformar_productos(ruta)

        df = pd.merge(productos, precios)
        df = df[['id_producto','Precio','Vencimiento']]
        return df
    
    def transformar_productos(self, ruta):
        return super().transformar_productos(ruta)
    
    def transformar_df(self, ruta):
        df = super().transformar_df(ruta)
        df = df.rename(columns={'Principal':'Producto'})

        return df
    
    def procesar_precios(self, ruta):
        return self.unir_tablas(ruta)

    
if __name__ == "__main__":

    load_dotenv()
    ruta_principal = os.getenv('RUTA_PRINCIPALES')
    instancia = TablaPrecios()
    df_transformado= instancia.procesar_precios(ruta_principal)
    print(df_transformado)
    # print(instancia.procesar_df(ruta_principal))