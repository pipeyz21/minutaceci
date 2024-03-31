import pandas as pd
import os
from tabla_base import TablaBase
from dotenv import load_dotenv

class TablaCostos(TablaBase):
    def transformar_df(self, ruta):
        return super().transformar_df(ruta)
    
    def _concatenar_compras(self, ruta_compras1, ruta_compras2, ruta_compras3):
        compras1 = self.transformar_df(ruta_compras1)
        compras2 = self.transformar_df(ruta_compras2)
        compras3 = self.transformar_df(ruta_compras3)
        df = pd.concat([compras1, compras2])
        df = df.drop(columns={'Proveedor.1', 'Nombre', 'Unidad'})

        df = df.dropna(subset='Producto')
        df['Dscto'] = df['Dscto'].fillna(0)
        df = df.rename(columns={'Compra':'Boleta'})

        return df
    
    def __estimar_costos_transporte(self, ruta_compras1, ruta_compras2, ruta_compras3, proveedores, insumos):
        costos = self._concatenar_compras(ruta_compras1, ruta_compras2, ruta_compras3)
        df = costos.merge(proveedores, how='inner', left_on='Proveedor',  right_on='ID_PROVEEDOR').merge(
                                                insumos, 
                                                how='inner', 
                                                left_on='Producto',
                                                right_on='ID_PRODUCTO')
    
        df = df[['Boleta','Fecha','NOMBRE_x','Producto','Total','SECTOR','TIPO']]
        fechas = df[df['Producto']==1004]['Fecha'].to_list()
        sectores_sin_pago = ['PEÑUELAS', 'LA CANTERA', 'DELIVERY']

        df1 = df[~(df['Fecha'].isin(fechas)) & (df['TIPO']!='SERVICIOS BÁSICOS') & ~(df['SECTOR'].isin(sectores_sin_pago))]
        df1 = df1.groupby(by=['Fecha','SECTOR']).agg({'Boleta':'nunique',
                                                    'NOMBRE_x':'nunique',
                                                    'Producto':'nunique',
                                                    'Total':'sum',
                                                    'TIPO':'nunique'}).reset_index()
        
        proveedores_sector = {'COQUIMBO':'T15', 'LA SERENA':'T33', 'LA GARZA':'T15', 'REGIMIENTO ARICA':'T33'}
        precio_proveedor = {'T15':1000, 'T33':1300}

        datos = []
        for i in range(len(df1)):
            
            proveedor = proveedores_sector.get(df1.loc[i, 'SECTOR'])
            precio = precio_proveedor.get(proveedor)
            
            if df1.loc[i, 'Producto'] > 10:
                cantidad = 4
            else:
                cantidad = 2

            datos.append({
                'Boleta': i,
                'Fecha': df1.loc[i, 'Fecha'],
                'Proveedor': proveedor,
                'Producto': 1004,
                'Cantidad': cantidad,
                'Precio_Unitario': precio,
                'Dscto': 0,
                'Total': precio*cantidad,
            })

            df = pd.DataFrame(datos)

        return df

    def procesar_costos(self, ruta_compras1, ruta_compras2, ruta_compras3, proveedores, insumos):
        costos = self._concatenar_compras(ruta_compras1, ruta_compras2, ruta_compras3)
        transporte = self.__estimar_costos_transporte(ruta_compras1, ruta_compras2, ruta_compras3, proveedores, insumos)

        df = pd.concat([costos, transporte])
        df = df.sort_values(by=['Fecha', 'Proveedor']).reset_index().drop(columns='index')

        df['aux'] = df.Fecha + "/" + df.Proveedor
        df['Boleta'] = 0

        df['aux2'] = (df['aux'] == df['aux'].shift(1))

        for i in range(1,len(df)):
            j = i-1

            if not df.loc[i, 'aux2']:
                df.loc[i, 'Boleta'] = df.loc[j, 'Boleta'] + 1
                
            else:
                df.loc[i, 'Boleta'] = df.loc[i - 1, 'Boleta'] 
        
        df = df.drop(columns={'aux','aux2'})

        df = self.aplicar_fechas(df)

        return df

