from sklearn import metrics
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
# from pmdarima.utils import diff, array, diff_inv
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor
from datetime import datetime

def mean_absolute_percentage_error(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100

def evaluacion_metrica(y_true, y_pred):
    print('Evaluation metric results:-')
    print(f'MSE is : {metrics.mean_squared_error(y_true, y_pred)}')
    print(f'MAE is : {metrics.mean_absolute_error(y_true, y_pred)}')
    print(f'RMSE is : {np.sqrt(metrics.mean_squared_error(y_true, y_pred))}')
    print(f'MAPE is : {mean_absolute_percentage_error(y_true, y_pred)}')
    print(f'R2 is : {metrics.r2_score(y_true, y_pred)}',end='\n\n')

def query(parametro):
    query = f'''
        SELECT 
            {parametro},
            Numero_de_Mes,
            Numero_de_Semana,
            sum(Cantidad) Cantidad
        FROM 
            (
                SELECT
                    Dia_del_Año,
                    Numero_de_Mes,
                    Numero_de_Semana,
                    Año,
                    Cantidad,
                    Total,
                    Fecha
                FROM ingresos
                WHERE 1=1
                    AND Año = 2023
                    AND Numero_de_Mes >= 3
                    AND Tipo LIKE "ALMUERZOS"
            ) as sub
        GROUP BY 1,2,3
    '''
    return query

if __name__ == '__main__':
    load_dotenv()

    url = f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    engine = create_engine(url=url)

    df = pd.read_sql(query('Fecha'), con=engine)
    # df = df[(df['Tipo']=='ALMUERZOS') & (df['Año']==2023) & (df['Numero_de_Mes']>2)]
    df = df[['Fecha','Cantidad','Numero_de_Mes','Numero_de_Semana']]
    df = df.groupby(by='Fecha').sum().reset_index()

    df['Fecha'] = pd.to_datetime(df['Fecha'])

    df = df.sort_values('Fecha')

    # print(df.head())

    rango_completo = pd.date_range(start=df['Fecha'].iloc[0], end=df['Fecha'].iloc[-1], freq='D')
    fechas_faltantes = set(rango_completo) - set(df['Fecha'])
    df_faltantes = pd.DataFrame({'Fecha': list(fechas_faltantes), 'Cantidad':df['Cantidad'].mean()})

    df = pd.concat([df, df_faltantes]).sort_values('Fecha').reset_index(drop=True)
    df = df.sort_values('Fecha')
    df = df.set_index('Fecha')

    df.index.freq = 'D'

    df['diff'] = df['Cantidad'].pct_change()
    df = df.dropna()    

    df['P1'] = df['diff'].shift(1)
    df['P2'] = df['diff'].shift(2)
    df['P3'] = df['diff'].shift(3)
    df['P4'] = df['diff'].shift(4)
    df['P5'] = df['diff'].shift(5)
    df['P6'] = df['diff'].shift(6)
    df['P7'] = df['diff'].shift(7)
    df = df.dropna()

    # print(df.head())

    x = df.loc[:, ['Numero_de_Semana','Numero_de_Mes','P1','P2','P3','P4','P5','P6','P7']]
    y = df.loc[:, 'Cantidad']

    x_train, y_train = x[x.index<'2023-12-20'], y[y.index<'2023-12-20']
    x_test, y_test = x[x.index>='2023-12-20'], y[y.index>='2023-12-20']

    print('Empieza el entrenamiento')
    t1 = datetime.utcnow()

    rfr = RandomForestRegressor(n_estimators = 100) #n° arboles
    
    rfr.fit(x_train, y_train)

    t2 = datetime.utcnow() - t1

    print(f'Entrenamiento Completado, tiempo total: {t2}')

    fcst = rfr.predict(x_test)

    test = pd.DataFrame(y_test)
    test['Random_Forest'] = fcst

    plt.rcParams["figure.figsize"] = (12, 4)

    plt.plot(test['Cantidad'],color="blue" ,label="Almuerzos")
    plt.plot(test['Random_Forest'], color="lime", label="Predicciones RF")
    plt.title("Predicción con Modelo RF", fontsize=30);
    plt.xlabel('Dias')
    plt.ylabel('')
    plt.legend( fontsize=16);
    plt.show();

    print(evaluacion_metrica(test['Cantidad'],test['Random_Forest']))