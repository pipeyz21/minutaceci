import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

class CargarDataFrame:
    def __init__(self) -> None:
        pass

    def _cargar_csv(self, df, ruta, nombre):
        df.to_csv(f'{ruta}/{nombre}.csv', index=False)
        

    def _cargar_sql(self, df, nombre):
        url = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        engine = create_engine(url=url)
        df.to_sql(nombre, con=engine, index=False, if_exists='replace')
    
    def _cargar_bigquery(self, df, nombre):
        pass

    def cargar_datos(self, df, ruta, nombre):
        self._cargar_csv(df, ruta, nombre)
        self._cargar_sql(df, nombre)
        print(f'¡Tabla {nombre} guardada en procesados y en SQL!')


