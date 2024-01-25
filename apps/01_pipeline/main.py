import transformar, cargar
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from extraer import ManejadorExcel

load_dotenv()

def extraer_datos():
    ruta_origen = os.getenv('RUTA_MINUTACECI')
    ruta_destino = os.getenv('RUTA_DESTINO')

    print('------------------Comienza la extracción de datos------------------')
    
    extraer = ManejadorExcel()

    print(ruta_origen)

    minutaceci = extraer.extraer_excel(ruta_origen, ruta_destino, 'minutaceci')

    if minutaceci is not None:
        extraer.extraer_tablas_excel(minutaceci)
    else:
        print("No se pudo extraer el archivo, la ruta de destino es None.")


def transformar_datos():
    print('----------------Comienza la transformación de datos----------------')

    df_para_trabajar = ['acompañamientos', 'almuerzos', 'compras1', 'compras2', 
                        'otros_ingresos', 'principales', 'productos', 'proveedores']
    
    ruta_tablas = os.getenv('RUTA_TABLAS')

    transformar.procesar_tablas(df_para_trabajar, ruta_tablas)
    ruta_almuerzos = os.getenv('RUTA_ALMUERZOS')
    ruta_otros = os.getenv('RUTA_OTROS')
    ruta_compras1 = os.getenv('RUTA_COMPRAS1')
    ruta_compras2 = os.getenv('RUTA_COMPRAS2')

    ingresos, precios, productos_ventas, clientes, acompañamientos, extras = transformar.calcular_ingresos(ruta_almuerzos, ruta_otros)
    costos = transformar.calcular_costos(ruta_compras1, ruta_compras2)

    return ingresos, precios, productos_ventas, clientes, acompañamientos, extras, costos

def cargar_datos(ingresos, precios, productos_ventas, clientes, acompañamientos, extras, costos):

    ruta_procesados = os.getenv('RUTA_PROCESADOS')
    ruta_productos = os.getenv('RUTA_PRODUCTOS')
    ruta_proveedores = os.getenv('RUTA_PROVEEDORES')

    print('---------------------Comienza la carga de datos---------------------')
    cargar.cargar_df_procesado(ingresos, ruta_procesados, 'ingresos')
    cargar.cargar_df_procesado(costos, ruta_procesados, 'costos')
    cargar.cargar_df_procesado(clientes, ruta_procesados, 'clientes')
    cargar.cargar_df_procesado(precios, ruta_procesados, 'precios')
    cargar.cargar_df_procesado(acompañamientos, ruta_procesados, 'acompañamientos')
    cargar.cargar_df_procesado(extras, ruta_procesados, 'extras')
    cargar.cargar_df_procesado(productos_ventas, ruta_procesados, 'productos_ventas')
    cargar.cargar_df_procesado(pd.read_csv(ruta_productos), ruta_procesados, 'productos_compras')
    cargar.cargar_df_procesado(pd.read_csv(ruta_proveedores), ruta_procesados, 'proveedores')


def main():
    t1 = datetime.utcnow()
    
    extraer_datos()
 
    ingresos, precios, productos_ventas, clientes, acompañamientos, extras, costos = transformar_datos()

    cargar_datos(ingresos, precios, productos_ventas, clientes, acompañamientos, extras, costos)

    t2 = datetime.utcnow() - t1
    
    with open("apps/01_pipeline/registro_tiempo.csv", "a") as file:
        # Escribe la línea con el ID y el tiempo de ejecución
        file.write(f"{t1.strftime('%Y-%m-%d %H:%M:%S')}, {t2} \n")

    print(f"----------Tiempo de procesamiento de pipeline: {t2}----------")

if __name__ == '__main__':
    main()