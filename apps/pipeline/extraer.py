import os
import shutil
import pandas as pd
import warnings

class ExtraerExcel:
    def __init__(self):
        pass  # Puedes inicializar cualquier estado necesario aquí

    def _copiar_excel(self, ruta_origen, ruta_destino, nombre_archivo):
        ruta_destino = f'{ruta_destino}/{nombre_archivo}.xlsx'
        if os.path.exists(ruta_origen):
            try:
                shutil.copy(ruta_origen, ruta_destino)
                print(f"Archivo {nombre_archivo} copiado con éxito ")
                return ruta_destino
            except Exception as e:
                print(f"Error al copiar el archivo: {e}")
        else:
            print("El archivo de origen no existe")
            return None

    def _extraer_tablas_excel(self, ruta_origen, ruta_destino):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message='Data Validation extension is not supported and will be removed')
            xls = pd.ExcelFile(ruta_origen)

            for nombre_hoja in xls.sheet_names:
                tabla = xls.parse(nombre_hoja)
                tabla.to_csv(f"{ruta_destino}/{nombre_hoja.lower()}.csv", index=False)
                print(f'Tabla {nombre_hoja.lower()} extraída')

    def extraer_excel(self, ruta_origen, ruta_destino, nombre_archivo):
        ruta = self._copiar_excel(ruta_origen, ruta_destino, nombre_archivo)
        ruta_crudos = ruta_destino

        if ruta is not None:
            self._extraer_tablas_excel(ruta, ruta_crudos)
        else:
            print("No se pudo extraer el archivo, la ruta de destino es None.")

class ExtraerArchivosTexto:
    def __init__(self) -> None:
        pass

    def _copiar_csv(self, ruta_origen, ruta_destino, nombre_archivo):
        ruta_destino = f"{ruta_destino}{nombre_archivo}.csv"
        if os.path.exists(ruta_origen):
            try:
                shutil.copy(ruta_origen, ruta_destino)
                print(f'Archivo {nombre_archivo} copiado con éxito')
            
            except Exception as e:
                print(f"Error al copiar el archivo: {e}")
        else:
            print("El archivo no existe")
        
    def _convertir_df(self, ruta_destino, nombre_archivo):
        ruta_destino = f"{ruta_destino}{nombre_archivo}.csv"
        try:
            df = pd.read_csv(ruta_destino)
            return df
        
        except Exception as e:
            print(f'No se pudo convertir a DataFrame, error{e}')
            return None

    def extraer_texto(self, ruta_origen, ruta_destino, nombre_archivo):
        self._copiar_csv(ruta_origen, ruta_destino, nombre_archivo)
        # df = self._convertir_df(ruta_destino, nombre_archivo)

        # return df


# Ejemplo de uso de la clase
# manejador = ManejadorExcel()
# manejador.extraer_excel("ruta_origen.xlsx", "ruta_destino.xlsx", "nombre_archivo")
