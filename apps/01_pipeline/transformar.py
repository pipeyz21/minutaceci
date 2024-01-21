import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv

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
    minutas = minutas.rename(columns={'index':'id'})

    precios = pd.merge(minutas, principales, 'inner' )
    precios = precios.drop(columns={'Principal','Categoria'})

    productos_ventas = productos.merge(minutas, how='left', right_on='Principal', left_on='Producto')
    productos_ventas = productos_ventas.sort_values('id').reset_index().drop(columns={'index', 'Principal'})
    max_id = productos_ventas['id'].max()

    indices_nulos = productos_ventas[(productos_ventas['id'].isna())].index

    productos_ventas = productos_ventas.merge(principales, 'left', left_on='Producto', right_on='Principal')

    productos_ventas['aux'] = productos_ventas.index * 1.0 + max_id
    productos_ventas.loc[indices_nulos, 'id'] = productos_ventas['aux']
    productos_ventas = productos_ventas.drop(columns={'aux','Principal','Precio','Vencimiento'})
    productos_ventas['Categoria'] = productos_ventas['Categoria'].replace(np.nan, 'OTROS') 
    
    #buscar precios de otros_ingresos
    
    return precios, productos_ventas

