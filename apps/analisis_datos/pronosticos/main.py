import numpy as np
import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.seasonal import seasonal_decompose
from dotenv import load_dotenv
from sqlalchemy import create_engine

def query(parametro):
    query = f'''
        SELECT 
            {parametro} Periodo,
            count(DISTINCT(Boleta)) boletas, 
            count(DISTINCT(id_producto)) productos,
            count(DISTINCT(id_cliente)) clientes,
            sum(Cantidad) almuerzos,
            sum(Total) ventas_clp
        FROM 
            (
                SELECT
                    Dia_del_Año,
                    Numero_de_Mes,
                    Numero_de_Semana,
                    Año,
                    id_cliente,
                    Boleta,
                    id_producto,
                    Cantidad,
                    Total
                FROM ingresos
                WHERE 1=1
                    AND Año = 2023
                    AND Numero_de_Mes >= 3
                    AND Tipo LIKE "ALMUERZOS"
            ) as sub
        GROUP BY 1
    '''
    return query

def evaluar_modelo(y_real, y_pred):
    print('Evaluando Modelo')
    # print(f'MSE: {metrics.}')
    print(f'MAPE = {MAPE(y_real, y_pred)}')

def MAPE(y_real, y_pred):
    y_true, y_pred = np.array(y_real), np.array(y_pred)
    return np.mean(np.abs((y_real - y_pred) / y_true))

def prueba_dickey_fuller(serie_temporal):
    print("Resultados de la prueba de Dickey-Fuller")
    # print(serie_temporal.head(5))
    dftest = adfuller(serie_temporal, autolag='AIC')
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic', 'p-value', 'No Lags Used', 'N° observaciones'])

    for key, value in dftest[4].items():
        dfoutput['Critical Value (%s)'%key] = value
    
    print(dfoutput)

    if dftest[1] <= 0.05:
        print('Conclusión:')
        print('Rechazar H0 - Los datos son estacionarios')
    else:
        print('Conclusión:')
        print('No rechazar H0 - Los datos no son estacionarios')


    

    print('--------------------------------FIN--------------------------------')

if __name__ == '__main__':
    load_dotenv()

    # Crear conexión con mySQL y traer datos al df
    url = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    engine = create_engine(url=url)

    # df_diario = pd.read_sql(query('Dia_del_Año'), con=engine)
    df_semana = pd.read_sql(query('Numero_de_Semana'), con=engine)
    # df_mes = pd.read_sql(query('Numero_de_Mes'), con=engine)

    # print('Evaluación Diaria')
    # prueba_dickey_fuller(df_diario['almuerzos'])
    

    print('Evaluación Semanal')
    # prueba_dickey_fuller(df_semana['almuerzos'])
    df_semana['almuerzos_diff'] = df_semana['almuerzos'].diff()
    df1 = df_semana.copy().dropna()
    prueba_dickey_fuller(df1['almuerzos_diff'])

    fig, ax = plt.subplots(figsize=(7, 3))
    # plot_acf(df1['almuerzos_diff'], ax=ax, lags=8)
    # plt.show()

    # plot_pacf(df1['almuerzos_diff'], ax=ax, lags=8)
    # plt.show()

    g = seasonal_decompose(df1['almuerzos_diff'], model='add', period=df1.index)
    g.plot()


    # print('Evaluación Mensual')
    # prueba_dickey_fuller(df_mes['almuerzos'])
    # df_mes['almuerzos_diff'] = df_mes['almuerzos'].diff()
    # df2 = df_mes.copy().dropna()
    # prueba_dickey_fuller(df2['almuerzos_diff'])
    




