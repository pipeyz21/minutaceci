import os
import pandas as pd
import sys
sys.path.append('apps/pipeline/transformar')
from dotenv import load_dotenv
from datetime import datetime
from extraer import ExtraerExcel, ExtraerArchivosTexto
from transformar import Transformar
from cargar import CargarDataFrame



load_dotenv()

def extraer_datos():

    print('------------------Comienza la extracción de datos------------------')
    
    excel = ExtraerExcel()
    texto = ExtraerArchivosTexto()

    excel.extraer_excel(os.getenv('RUTA_MINUTACECI'), os.getenv('RUTA_DESTINO'), 'minutaceci') 
    texto.extraer_texto(os.getenv('RUTA_CHAT_WSP'), os.getenv('RUTA_DESTINO'), 'oferta_sin_procesar')


def transformar_datos():
    print('----------------Comienza la transformación de datos----------------')
    transformador = Transformar()
    lista = transformador.procesar_datos()
    return lista


def cargar_datos(lista):

    nombres = ['acompañamientos', 'arriendos', 'clientes', 'costos', 'extras', 'ingresos', 'insumos', 'precios', 'productos', 'proveedores']
    cargar = CargarDataFrame()
    
    print('---------------------Comienza la carga de datos---------------------')
    for i in range(len(lista)):
        df = lista[i]
        cargar.cargar_datos(df, os.getenv('RUTA_PROCESADOS'), nombres[i])
        

        

def registro_tiempo(t1:datetime, t2:datetime):
    with open(os.getenv('RUTA_REGISTROS_PIPELINE'), 'a') as file:
        file.write(f"{t1.strftime('%Y-%m-%d %H:%M:%S')}, {t2} \n")

def main():
    t1 = datetime.utcnow()
    
    extraer_datos()
 
    # lista = transformar1_datos()
    lista = transformar_datos()

    cargar_datos(lista)

    t2 = datetime.utcnow() - t1
    
    registro_tiempo(t1, t2)

    print(f"----------Tiempo de procesamiento de pipeline: {t2}----------")

if __name__ == '__main__':
    main()