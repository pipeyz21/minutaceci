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
    df = pd.read_csv(f"{ruta}{tabla}.csv")
    df = limpieza_df(df)
    return df

def limpieza_df(df):
    df = df.map(lambda x: x.upper() if isinstance(x, str) else x)
    
    if 'Fecha' in df.columns:
        df = df.dropna(subset=['Fecha'])

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

    df = pd.concat([almuerzos, otros], sort=False)

    df['Cliente'] = df['Cliente'].fillna('???')
    df['Ensalada'] = df['Ensalada'].fillna('S/E')
    df['Acompañamiento'] = df['Acompañamiento'].fillna('SIN ACOM.')
    
    ruta_principales = os.getenv('RUTA_PRINCIPALES')
    precios, productos = calcular_productos(df, ruta_principales)

    clientes = pd.DataFrame(df['Cliente'].drop_duplicates()).reset_index()
    clientes = clientes.drop(columns='index').reset_index().rename(columns={'index':'id_cliente'})

    acompañamientos = pd.DataFrame(df['Acompañamiento'].drop_duplicates()).reset_index()
    acompañamientos = acompañamientos.drop(columns='index').reset_index().rename(columns={'index':'id_acompañamiento'})

    extras = pd.DataFrame(df['Ensalada'].drop_duplicates()).reset_index()
    extras = extras.drop(columns='index').reset_index().rename(columns={'index':'id_extra'})
    
    df = df.merge(clientes, 'left').merge(productos, 'left').merge(acompañamientos, 'left').merge(extras, 'left')
    df = df.rename(columns={'id':'id_producto'}).drop(columns={'Producto','Categoria','Cliente','Acompañamiento','Ensalada'})
    # df = periodos_tiempo(df, 'Fecha')

    ingresos = df.copy()
    ingresos = transformar_ingresos(ingresos)
    productos = transformar_productos(productos)

    return ingresos, precios, productos, clientes, acompañamientos, extras

def calcular_costos(ruta_compras1, ruta_compras2, ruta_transporte):
    compras1 = pd.read_csv(ruta_compras1)
    compras2 = pd.read_csv(ruta_compras2)
    
    transporte = pd.read_csv(ruta_transporte)

    max_boletas = compras1['Compra'].max() + 1
    compras2['Compra'] += max_boletas

    df = pd.concat([compras1, compras2])
    df = df.drop(columns={'Proveedor.1', 'Nombre', 'Unidad'})

    df = df.dropna(subset='Producto')
    df['Dscto'] = df['Dscto'].fillna(0)
    df = df.rename(columns={'Compra':'Boleta'})

    df = pd.concat([df, transporte])

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

    return df

def calcular_productos(ingresos, ruta_principales):
    productos = pd.DataFrame(ingresos['Producto'].drop_duplicates())

    principales = pd.read_csv(ruta_principales)

    minutas = pd.DataFrame(principales['Principal'].drop_duplicates()).reset_index()
    minutas = minutas.rename(columns={'index':'id_producto'})

    precios = pd.merge(minutas, principales, 'inner' )
    precios = precios.drop(columns={'Principal','Categoria'})

    productos = productos.merge(minutas, how='left', right_on='Principal', left_on='Producto')
    productos = productos.sort_values('id_producto').reset_index().drop(columns={'index', 'Principal'})
    max_id = productos['id_producto'].max()

    indices_nulos = productos[(productos['id_producto'].isna())].index

    productos['aux'] = productos.index * 1.0 + max_id
    productos.loc[indices_nulos, 'id_producto'] = productos['aux']
    productos = productos.drop(columns={'aux'})
    productos = productos.merge(principales, 'left', left_on='Producto', right_on='Principal')
    productos['Categoria'] = productos['Categoria'].replace(np.nan, 'OTROS') 
    productos = productos.drop_duplicates(subset='id_producto').drop(columns={'Principal','Precio','Vencimiento'})
    
    #buscar precios de otros_ingresos
    
    return precios, productos

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

def calcular_arriendos(df):
    ruta_subsidio = os.getenv('RUTA_SUBSIDIO')

    arriendos = pd.read_csv(ruta_subsidio)
    arriendos = arriendos[['Fecha','Subsidio','Copago','Contrato2']]
    arriendos = periodos_tiempo(arriendos, 'Fecha')

    return arriendos

def transformar_ingresos(df):
    
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

def transformar_productos(productos):
    productos['Envase'] = np.nan

    ct5 = productos[productos['Producto']=='ZAPALLO ITALIANO'].index
    productos.loc[ct5, 'Envase'] = 141

    c18 = productos[(productos['Producto'].str.startswith('LASAÑA')) | (productos['Categoria']=='PASTELES')].index
    productos.loc[c18, 'Envase'] = 7

    marmitas = productos[(productos['Envase'].isna()) & (productos['Categoria'] != 'OTROS')].index
    productos.loc[marmitas, 'Envase'] = 3

    return productos

def encontrar_favoritos(lista):
    datos = pd.Series(lista).value_counts().reset_index().rename(columns={'index': 'id', 0: 'cantidad'})

    return datos.iloc[0, 0]

def indicadores_clientes(clientes, ingresos, Año=None):

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

        df_clientes.loc[id, 'producto_favorito'] = encontrar_favoritos(productos_favoritos)
        df_clientes.loc[id, 'acompañamiento_favorito'] = encontrar_favoritos(acompañamientos_favoritos)
        df_clientes.loc[id, 'extra_favorito'] = encontrar_favoritos(extras_favoritos)
        df_clientes.loc[id, 'frecuencia_compra_dif'] = dif_fechas

    df_clientes['tiempo_cliente'] = (df_clientes['ultima_compra'] - df_clientes['primera_compra']).dt.days
    df_clientes['frecuencia_compra_resta'] = (df_clientes['tiempo_cliente'])/df_clientes['boletas']
    df_clientes['ticket_promedio'] = df_clientes['gasto_total']/df_clientes['boletas']
        
    return df_clientes.dropna()

def estimar_gastos_transporte():

    costos = pd.read_csv(os.getenv('RUTA_COSTOS'))
    proveedores = pd.read_csv(os.getenv('RUTA_PROVEEDORES'))
    insumos = pd.read_csv(os.getenv('RUTA_INSUMOS'))


    df = costos.merge(proveedores, 
                  how='inner', 
                  left_on='Proveedor', 
                  right_on='ID_PROVEEDOR').merge(insumos, 
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

        datos = pd.DataFrame(datos)

    return datos