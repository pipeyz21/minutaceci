import pandas as pd
import os
from ingresos import TablaIngresos
from dotenv import load_dotenv

class TablaClientes(TablaIngresos):
    def transformar_clientes(self, ruta_almuerzos, ruta_otros):
        df = self.transformar_ingresos(ruta_almuerzos, ruta_otros)
        df = pd.DataFrame(df['Cliente'].drop_duplicates()).reset_index()
        df = df.drop(columns='index').reset_index().rename(columns={'index':'id_cliente'})
        return df
    
    def procesar_clientes(self, ruta_almuerzos, ruta_otros):
        return self.transformar_clientes(ruta_almuerzos, ruta_otros)
    
    def encontrar_favoritos(self, lista):
        datos = pd.Series(lista).value_counts().reset_index().rename(columns={'index': 'id', 0: 'cantidad'})
        return datos.iloc[0, 0]
    
    def indicadores_clientes(self, clientes, ingresos, Año=None):
        df = ingresos.copy()
        df_clientes = clientes.copy()

        if not Año is None:
            df = df[df['Año']==Año]

        df1 = df[['Boleta','Fecha','id_cliente','Tipo','id_producto','id_acompañamiento','id_extra','Cantidad','Total']]

        for id, df2 in df1.groupby('id_cliente'): #
                
            fechas = set(df2['Fecha'].to_list())

            if len(fechas)>1:

                fechas = pd.DataFrame(fechas).rename(columns={0:'Fecha'}).sort_values('Fecha').reset_index().drop(columns='index')
                fechas['Dif'] = fechas['Fecha'].diff().dt.days

                dif_fechas = fechas['Dif'].sum()/(fechas.shape[0] - 1)
            
            else: 
                dif_fechas = 0.

            id = int(id)

            df_clientes.loc[id, 'primera_compra'] = df2['Fecha'].min()
            df_clientes.loc[id, 'ultima_compra'] = df2['Fecha'].max()

            df3 = df2[['Fecha','Boleta','Total']].groupby(['Boleta','Fecha']).sum().reset_index()
            df_clientes.loc[id, 'compra_minima'] = df3['Total'].min()
            df_clientes.loc[id, 'compra_maxima'] = df3['Total'].max()
            df_clientes.loc[id, 'gasto_total'] = df3['Total'].sum()
            df_clientes.loc[id, 'boletas'] = df3['Boleta'].nunique()

            almuerzos = df2[df2['Tipo']=='ALMUERZOS']

            if not almuerzos.empty:
                df_clientes.loc[id, 'gasto_almuerzos'] = almuerzos['Total'].sum()
                df_clientes.loc[id, 'cantidad_almuerzos'] = almuerzos['Cantidad'].sum()
            
            else:
                df_clientes.loc[id, 'gasto_almuerzos'] = 0
                df_clientes.loc[id, 'cantidad_almuerzos'] = 0

            otros = df2[df2['Tipo']=='OTROS']

            if not otros.empty:
                df_clientes.loc[id, 'gasto_otros'] = otros['Total'].sum()
                df_clientes.loc[id, 'cantidad_otros'] = otros['Cantidad'].sum()
            
            else:
                df_clientes.loc[id, 'gasto_otros'] = 0
                df_clientes.loc[id, 'cantidad_otros'] = 0

            productos_favoritos = df2['id_producto'].to_list()
            acompañamientos_favoritos = df2['id_acompañamiento'].to_list()
            extras_favoritos = df2['id_extra'].to_list()

            df_clientes.loc[id, 'producto_favorito'] = self.encontrar_favoritos(productos_favoritos)
            df_clientes.loc[id, 'acompañamiento_favorito'] = self.encontrar_favoritos(acompañamientos_favoritos)
            df_clientes.loc[id, 'extra_favorito'] = self.encontrar_favoritos(extras_favoritos)
            df_clientes.loc[id, 'frecuencia_compra_dif'] = dif_fechas

        df_clientes['tiempo_cliente'] = (df_clientes['ultima_compra'] - df_clientes['primera_compra']).dt.days
        df_clientes['frecuencia_compra_resta'] = (df_clientes['tiempo_cliente'])/df_clientes['boletas']
        df_clientes['ticket_promedio'] = df_clientes['gasto_total']/df_clientes['boletas']
            
        return df_clientes.dropna()