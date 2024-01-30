import transformar
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from extraer import ExtraerExcel
from cargar import CargarDataFrame

load_dotenv()

def extraer_datos():

    print('------------------Comienza la extracción de datos------------------')
    
    extraer = ExtraerExcel()

    extraer.extraer_excel(os.getenv('RUTA_MINUTACECI'), os.getenv('RUTA_DESTINO'), 'minutaceci') 



def transformar_datos():
    print('----------------Comienza la transformación de datos----------------')

    df_para_trabajar = ['acompañamientos', 'subsidio', 'almuerzos', 'compras1', 'compras2', 
                        'otros_ingresos', 'principales', 'productos', 'proveedores']
    

    transformar.procesar_tablas(df_para_trabajar, os.getenv('RUTA_DESTINO'))
  
    ingresos, precios, productos, clientes, acompañamientos, extras = transformar.calcular_ingresos(os.getenv('RUTA_ALMUERZOS'), 
                                                                                                    ruta_otros = os.getenv('RUTA_OTROS'))
    

    costos = transformar.calcular_costos(os.getenv('RUTA_COMPRAS1'), os.getenv('RUTA_COMPRAS2'), os.getenv('RUTA_TRANSPORTE'))
    arriendos = transformar.calcular_arriendos(os.getenv('RUTA_SUBSIDIO'))
    

    return [ingresos, precios, productos, clientes, acompañamientos, extras, costos, arriendos]

def cargar_datos(lista):

    nombres = ['ingresos', 'precios', 'productos', 'clientes', 'acompañamientos', 'extras', 'costos', 'arriendos']
    cargar = CargarDataFrame()
    
    print('---------------------Comienza la carga de datos---------------------')
    for i in range(len(lista)):
        df = lista[i]
        cargar.cargar_datos(df, os.getenv('RUTA_PROCESADOS'), nombres[i])
        
        
    cargar.cargar_datos(pd.read_csv(os.getenv('RUTA_MATERIAL')), os.getenv('RUTA_PROCESADOS'), 'insumos')
    cargar.cargar_datos(pd.read_csv(os.getenv('RUTA_PROVEEDORES')), os.getenv('RUTA_PROCESADOS'), 'proveedores')

        

def registro_tiempo(t1:datetime, t2:datetime):
    with open(os.getenv('RUTA_REGISTROS_PIPELINE'), 'a') as file:
        file.write(f"{t1.strftime('%Y-%m-%d %H:%M:%S')}, {t2} \n")

def main():
    t1 = datetime.utcnow()
    
    extraer_datos()
 
    lista = transformar_datos()

    cargar_datos(lista)

    t2 = datetime.utcnow() - t1
    
    registro_tiempo(t1, t2)

    print(f"----------Tiempo de procesamiento de pipeline: {t2}----------")

if __name__ == '__main__':
    main()