import pandas as pd
import os
import numpy as np
from dotenv import load_dotenv
from ingresos import TablaIngresos
from principales import TablaPrincipales

class TablaProductos(TablaIngresos, TablaPrincipales):
    def __init__(self):
        super().__init__()

    def transformar_productos(self, ruta_principal):
        df = self.procesar_principales(ruta_principal)

        df = df[['Categoria','Producto']]
        df = df.drop_duplicates().reset_index().drop(columns={'index'})
        df = df.reset_index().rename(columns={'index':'id_producto'})

        return df

    def __agregar_envases(self, productos):
        productos['Envase'] = np.nan

        ct5 = productos[productos['Producto']=='ZAPALLO ITALIANO'].index
        productos.loc[ct5, 'Envase'] = 141

        c18 = productos[(productos['Producto'].str.startswith('LASAÑA')) | (productos['Categoria']=='PASTELES')].index
        productos.loc[c18, 'Envase'] = 7

        marmitas = productos[(productos['Envase'].isna()) & (productos['Categoria'] != 'OTROS')].index
        productos.loc[marmitas, 'Envase'] = 3

        return productos

    def procesar_productos(self, ruta_principal, ruta_almuerzos, ruta_otros):
        productos = self.transformar_productos(ruta_principal)

        ingresos = self.transformar_ingresos(ruta_almuerzos, ruta_otros)
        ingresos = ingresos[['Producto']].drop_duplicates()

        df = pd.merge(ingresos, productos, how='outer')
        df = df.sort_values('id_producto').reset_index()

        indices_nulos = df[(df['id_producto'].isna())].index
        df.loc[indices_nulos, 'id_producto'] = df.loc[indices_nulos, :].index
        df.loc[indices_nulos, 'Categoria'] = 'OTROS'
        
        df = df.drop(columns={'index'})
        df = self.__agregar_envases(df)

        return df
    

    
if __name__ == "__main__":

    load_dotenv()
    ruta_principal = os.getenv('RUTA_PRINCIPALES')
    ruta_crudos = os.getenv('RUTA_DESTINO')

    ruta_almuerzos = os.getenv('RUTA_ALMUERZOS')
    ruta_otros = os.getenv('RUTA_OTROS')

    instancia = TablaProductos()
    df_transformado = instancia.procesar_productos(ruta_principal, ruta_almuerzos, ruta_otros)
    print(df_transformado)