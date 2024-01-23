import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from datetime import timedelta

load_dotenv()

def periodos_tiempo(df, columna_fecha):
    df[columna_fecha] = pd.to_datetime(df[columna_fecha])
    df['Año'] = df[columna_fecha].dt.year
    df['Mes'] = df[columna_fecha].dt.month
    df['Trimestre'] = df[columna_fecha].dt.quarter
    df['MesNombre'] = df[columna_fecha].dt.strftime('%b')
    df['Semana'] = df[columna_fecha].dt.strftime('%W')
    df['Dia'] = df[columna_fecha].dt.day
    df['DiaAño'] = df[columna_fecha].dt.dayofyear
    df['DiaSemana'] = df[columna_fecha].dt.strftime('%A')

    return df

def cargar_y_limpiar(ruta, tabla):
    df = pd.read_csv(f"{ruta}/{tabla}.csv")
    df = limpieza_df(df)
    return df

def limpieza_df(df):
    df = df.map(lambda x: x.upper() if isinstance(x, str) else x)
    
    if 'Fecha' in df.columns:
        df = df.dropna(subset='Fecha')

    return df

def procesar_tablas(nombres_tablas, ruta):
    for tabla in nombres_tablas:
        df = cargar_y_limpiar(ruta, tabla)
        df.to_csv(f"{ruta}/{tabla}.csv", index=False)
        print(f"Tabla {tabla} procesada y guardada.")

def calcular_ingresos(ruta_almuerzos, ruta_otros):
    almuerzos = pd.read_csv(ruta_almuerzos)
    almuerzos = almuerzos.rename(columns={'Nombre_Cliente':'Cliente',
                                      'Principal':'Producto'})
    almuerzos['Tipo'] = 'ALMUERZOS'

    otros = pd.read_csv(ruta_otros)
    otros = otros.rename(columns={'Producto':'Producto'})
    otros['Tipo'] = 'OTROS'

    max_boletas = almuerzos['Boleta'].max() + 1
    otros['Boleta'] += max_boletas 

    df = pd.concat([almuerzos, otros], sort=False)

    df['Cliente'] = df['Cliente'].replace(np.nan, '???')
    df['Ensalada'] = df['Ensalada'].replace(np.nan, 'S/E')
    df['Acompañamiento'] = df['Acompañamiento'].replace(np.nan, 'SIN ACOM.')
    
    ruta_principales = os.getenv('RUTA_PRINCIPALES')
    precios, productos_ventas = calcular_productos_ventas(df, ruta_principales)

    clientes = pd.DataFrame(df['Cliente'].drop_duplicates()).reset_index()
    clientes = clientes.drop(columns='index').reset_index().rename(columns={'index':'id_cliente'})

    acompañamientos = pd.DataFrame(df['Acompañamiento'].drop_duplicates()).reset_index()
    acompañamientos = acompañamientos.drop(columns='index').reset_index().rename(columns={'index':'id_acompañamiento'})

    extras = pd.DataFrame(df['Ensalada'].drop_duplicates()).reset_index()
    extras = extras.drop(columns='index').reset_index().rename(columns={'index':'id_extra'})
    
    df = df.merge(clientes, 'left')
    df = df.merge(productos_ventas, 'left')
    df = df.merge(acompañamientos, 'left')
    df = df.merge(extras, 'left')
    df = df.rename(columns={'id':'id_producto'})
    df = df.drop(columns={'Producto','Categoria','Cliente','Acompañamiento','Ensalada'})
    df = periodos_tiempo(df, 'Fecha')

    ingresos = df.copy()
    ingresos['Fecha'] = pd.to_datetime(ingresos['Fecha'])

    ingresos = transformar_nulos_ingresos(ingresos)
    clientes = indicadores_clientes(clientes, ingresos)

    return ingresos, precios, productos_ventas, clientes, acompañamientos, extras

def calcular_costos(ruta_compras1, ruta_compras2):
    compras1 = pd.read_csv(ruta_compras1)
    compras2 = pd.read_csv(ruta_compras2)

    max_boletas = compras1['Compra'].max() + 1
    compras2['Compra'] += max_boletas

    df = pd.concat([compras1, compras2])
    df = df.drop(columns={'Proveedor.1', 'Nombre', 'Unidad'})
    df = periodos_tiempo(df, 'Fecha')

    return df

def calcular_productos_ventas(ingresos, ruta_principales):
    productos = pd.DataFrame(ingresos['Producto'].drop_duplicates())

    principales = pd.read_csv(ruta_principales)

    minutas = pd.DataFrame(principales['Principal'].drop_duplicates()).reset_index()
    minutas = minutas.rename(columns={'index':'id_producto'})

    precios = pd.merge(minutas, principales, 'inner' )
    precios = precios.drop(columns={'Principal','Categoria'})

    productos_ventas = productos.merge(minutas, how='left', right_on='Principal', left_on='Producto')
    productos_ventas = productos_ventas.sort_values('id_producto').reset_index().drop(columns={'index', 'Principal'})
    max_id = productos_ventas['id_producto'].max()

    indices_nulos = productos_ventas[(productos_ventas['id_producto'].isna())].index

    productos_ventas['aux'] = productos_ventas.index * 1.0 + max_id
    productos_ventas.loc[indices_nulos, 'id_producto'] = productos_ventas['aux']
    productos_ventas = productos_ventas.drop(columns={'aux'})
    productos_ventas = productos_ventas.merge(principales, 'left', left_on='Producto', right_on='Principal')
    productos_ventas['Categoria'] = productos_ventas['Categoria'].replace(np.nan, 'OTROS') 
    productos_ventas = productos_ventas.drop_duplicates(subset='id_producto').drop(columns={'Principal','Precio','Vencimiento'})
    
    #buscar precios de otros_ingresos
    
    return precios, productos_ventas

def calcular_precios_cercanos(df, columna_precio):
    precios = df[columna_precio].to_list()
    precios = [precio for precio in precios if precio > 0]

    if len(precios) > 1:
        return sum(precios)/len(precios)
    
    elif len(precios) == 1:
        return precios[0]

    else: 
        # Buscar rangos de fechas más largos
        return 0

def transformar_nulos_ingresos(df):
    
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
        fecha_min = fecha[0] - timedelta(7)
        fecha_max = fecha[0] + timedelta(7)

        df4 = df.copy()

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

            precio_final = calcular_precios_cercanos(df4, 'Precio')

        else:
            precio_final = calcular_precios_cercanos(df4, 'Precio')

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

    return df

def encontrar_favoritos(lista):
    datos = pd.Series(lista).value_counts().reset_index().rename(columns={'index': 'id', 0: 'cantidad'})

    return datos.iloc[0, 0]

def indicadores_clientes(clientes, ingresos):

    df1 = ingresos[['Boleta','Fecha','id_cliente','Tipo','id_producto','id_acompañamiento','id_extra','Cantidad','Total']]

    # lista_clientes = clientes['id_cliente'].to_list()

    for id, df2 in df1.groupby('id_cliente'): #
        
        fechas = set(df2['Fecha'].to_list())
        if len(fechas)>1:

            fechas = pd.DataFrame(fechas).rename(columns={0:'Fecha'}).sort_values('Fecha').reset_index().drop(columns='index')
            fechas['Dif'] = fechas['Fecha'].diff().dt.days

            dif_fechas = fechas['Dif'].sum()/(fechas.shape[0] - 1)
        
        else: 
            dif_fechas = 0.


        clientes.loc[id, 'primera_compra'] = df2['Fecha'].min()
        clientes.loc[id, 'ultima_compra'] = df2['Fecha'].max()

        df3 = df2[['Fecha','Boleta','Total']].groupby(['Boleta','Fecha']).sum().reset_index()
        clientes.loc[id, 'compra_minima'] = df3['Total'].min()
        clientes.loc[id, 'compra_maxima'] = df3['Total'].max()
        clientes.loc[id, 'gasto_total'] = df3['Total'].sum()
        clientes.loc[id, 'boletas'] = df3['Boleta'].nunique()

        almuerzos = df2[df2['Tipo']=='ALMUERZOS']

        if not almuerzos.empty:
            clientes.loc[id, 'gasto_almuerzos'] = almuerzos['Total'].sum()
            clientes.loc[id, 'cantidad_almuerzos'] = almuerzos['Cantidad'].sum()
        
        else:
            clientes.loc[id, 'gasto_almuerzos'] = 0
            clientes.loc[id, 'cantidad_almuerzos'] = 0

        otros = df2[df2['Tipo']=='OTROS']

        if not otros.empty:
            clientes.loc[id, 'gasto_otros'] = otros['Total'].sum()
            clientes.loc[id, 'cantidad_otros'] = otros['Cantidad'].sum()
        
        else:
            clientes.loc[id, 'gasto_otros'] = 0
            clientes.loc[id, 'cantidad_otros'] = 0

        productos_favoritos = df2['id_producto'].to_list()
        acompañamientos_favoritos = df2['id_acompañamiento'].to_list()
        extras_favoritos = df2['id_extra'].to_list()

        clientes.loc[id, 'producto_favorito'] = encontrar_favoritos(productos_favoritos)
        clientes.loc[id, 'acompañamiento_favorito'] = encontrar_favoritos(acompañamientos_favoritos)
        clientes.loc[id, 'extra_favorito'] = encontrar_favoritos(extras_favoritos)
        clientes.loc[id, 'frecuencia_compra_dif'] = dif_fechas

    clientes['tiempo_cliente'] = (clientes['ultima_compra'] - clientes['primera_compra']).dt.days
    clientes['frecuencia_compra_resta'] = (clientes['tiempo_cliente'])/clientes['boletas']
    clientes['ticket_promedio'] = clientes['gasto_total']/clientes['boletas']
        
    return clientes