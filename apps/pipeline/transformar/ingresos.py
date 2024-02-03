import pandas as pd
import os
from datetime import timedelta
from dotenv import load_dotenv
from almuerzos import TablaAlmuerzos
from otros import TablaOtros


class TablaIngresos(TablaAlmuerzos, TablaOtros):
    def __init__(self):
        super().__init__()

    def concatenar_tablas(self, ruta_almuerzos, ruta_otros):

        df_almuerzos = self.procesar_almuerzos(ruta_almuerzos)
        df_otros = self.procesar_otros(ruta_otros)

        df_almuerzos['Tipo'] = 'ALMUERZOS'
        df_otros['Tipo'] = 'OTROS'

        df_concatenado = pd.concat([df_almuerzos, df_otros], sort=False)
        return df_concatenado

    def transformar_ingresos(self, ruta_almuerzos, ruta_otros):
        df = self.concatenar_tablas(ruta_almuerzos, ruta_otros)

        df['Cliente'] = df['Cliente'].fillna('???')
        df['Ensalada'] = df['Ensalada'].fillna('S/E')
        df['Acompañamiento'] = df['Acompañamiento'].fillna('SIN ACOM.')

        return df
    
    def calcular_precios_cercanos(self, df, columna_precio):
        precios = df[columna_precio].to_list()
        precios = [precio for precio in precios if precio > 0]

        if len(precios) > 1:
            return sum(precios)/len(precios)
        
        elif len(precios) == 1:
            return precios[0]

        else: 
            # Buscar rangos de fechas más largos
            return 0

    def procesar_ingresos(self, ruta_almuerzos, ruta_otros, clientes, productos, acompañamientos, extras):
        ingresos = self.transformar_ingresos(ruta_almuerzos, ruta_otros)
        df = self.intersectar_tablas(ingresos, clientes, productos, acompañamientos, extras)

        df1 = df[ (df['Descuento'].isna()) | (df['Total'].isna()) | (df['Precio'].isna()) ].copy() # subconsulta con los nulos

        df_precios_na = df1[(df1['Precio'].isna())]

        df2 = df_precios_na.copy()
        indices_precios_na = df2.index.to_list()
        cantidades = df2.loc[indices_precios_na, 'Cantidad'].to_list()
        productos = df2.loc[indices_precios_na, 'id_producto'].to_list()

        datos = []

        # No existe diferenciación entre productos de almuerzos y otros.

        for i in range(len(productos)):
            df3 = df2[(df2['id_producto']==productos[i])]
            indice = df3.index.to_list()
            
            fecha = df2.loc[indice, 'Fecha'].to_list()
            fecha = pd.to_datetime(fecha)

            fecha_min = fecha[0] - timedelta(7)
            fecha_max = fecha[0] + timedelta(7)

            df4 = df.copy()
            df4['Fecha'] = pd.to_datetime(df4['Fecha'])

            # Falta calcular precios para productos de almuerzos 

            condicion1 = df4['id_producto'].isin(productos)
            condicion2 = df4['Fecha'] > fecha_min
            condicion3 = df4['Fecha'] < fecha_max
            condicion4 = df4['Cantidad'].isin(cantidades)

            df4 = df4[(condicion1) & (condicion2) & (condicion3) & (condicion4)]
            precio_final = 0

            if df4.empty:
                df4 = df.copy()
                df4 = df4[(condicion1) & (condicion2) & (condicion3)]

                precio_final = self.calcular_precios_cercanos(df4, 'Precio')

            else:
                precio_final = self.calcular_precios_cercanos(df4, 'Precio')

            datos.append({
                    'Indice':indice[0],
                    'id_producto': productos[i],
                    'Precio': precio_final
                })

        datos = pd.DataFrame(datos)

        if not datos.empty:
            datos = datos.set_index('Indice')
            df1.loc[datos.index.to_list(), ['Principal-Producto', 'Precio']] = datos

        df1.loc[:,('Descuento')] = df1.loc[:, ('Descuento')].str.replace('}', '', regex=True)
        df1['Descuento'] = df1['Descuento'].fillna(0)
        df1['Descuento'] = df1['Descuento'].astype(float)

        df1['Total'] = (df1['Cantidad'] * df1['Precio']) - df1['Descuento']

        df.loc[df1.index.to_list(), :] = df1
        df = self.aplicar_fechas(df)

        return df
    
    def intersectar_tablas(self, ingresos, clientes, productos, acompañamientos, extras):
        df = ingresos.merge(clientes, 'left').merge(productos, 'left').merge(acompañamientos, 'left').merge(extras, 'left')
        return df[['Boleta','Fecha','Tipo','id_cliente','id_producto',
                   'id_acompañamiento','id_extra','Cantidad','Precio','Descuento','Total']]

