def cargar_df_procesado(df, ruta, nombre):
    df.to_csv(f'{ruta}/{nombre}.csv', index=False)
    print(f'Tabla {nombre} guardada en procesados!')