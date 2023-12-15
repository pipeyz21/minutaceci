import pandas as pd
import matplotlib.pyplot as plt
import warnings

PATH = "minutaceci.xlsx"

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message="Data Validation extension is not supported and will be removed")
    almuerzos = pd.read_excel(PATH, sheet_name='Almuerzos')
    categorias = pd.read_excel(PATH, sheet_name='Categorias')
    otros = pd.read_excel(PATH, sheet_name='Otros_Ingresos')

# var = almuerzos.plot()
ventas = almuerzos.copy()
ventas = ventas[ventas['Fecha']>='2023-03-01']

otros = otros[otros['Fecha']>'2023-03-01']
