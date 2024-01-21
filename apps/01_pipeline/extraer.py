import shutil
import os
import pandas as pd
import warnings

# Extracción de datos Excel
def extraer_excel(ruta_origen, ruta_destino):

    if os.path.exists(ruta_origen):
        try:
            shutil.copy(ruta_origen, ruta_destino)
            print("Archivo copiado con exito ")
        
        except Exception as e:
            print(f"Error al copar el archivo: {e}")

    else:
        print("El archivo de origen no existe")

def extraer_tablas_excel(ruta_excel):

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message='Data Validation extension is not supported and will be removed')
        xls = pd.ExcelFile(ruta_excel)

        for nombre_hoja in xls.sheet_names:
            tabla = xls.parse(nombre_hoja)
            tabla.to_csv(f"./datos/crudos/{nombre_hoja.lower()}.csv", index=False)
            print(f'Tabla {nombre_hoja.lower()} extraída')