import os
import shutil
import pandas as pd
import warnings

class ManejadorExcel:
    def __init__(self):
        pass  # Puedes inicializar cualquier estado necesario aquí

    def extraer_excel(self, ruta_origen, ruta_destino, nombre_archivo):
        ruta_destino = f'{ruta_destino}/{nombre_archivo}.xlsx'
        if os.path.exists(ruta_origen):
            try:
                shutil.copy(ruta_origen, ruta_destino)
                print("Archivo copiado con éxito ")
                return ruta_destino
            except Exception as e:
                print(f"Error al copiar el archivo: {e}")
        else:
            print("El archivo de origen no existe")
            return None

    def extraer_tablas_excel(self, ruta_excel):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message='Data Validation extension is not supported and will be removed')
            xls = pd.ExcelFile(ruta_excel)

            for nombre_hoja in xls.sheet_names:
                tabla = xls.parse(nombre_hoja)
                tabla.to_csv(f"./datos/crudos/{nombre_hoja.lower()}.csv", index=False)
                print(f'Tabla {nombre_hoja.lower()} extraída')

# Ejemplo de uso de la clase
# manejador = ManejadorExcel()
# ruta = manejador.extraer_excel("ruta_origen.xlsx", "ruta_destino.xlsx")
# manejador.extraer_tablas_excel(ruta)
