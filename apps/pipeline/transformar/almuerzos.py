from tabla_base import TablaBase

class TablaAlmuerzos(TablaBase):
    def transformar_df(self, ruta):
        df = super().transformar_df(ruta)
        df = df.rename(columns={'Nombre_Cliente':'Cliente',
                                      'Principal':'Producto'})

        return df
    
    def procesar_almuerzos(self, ruta):
        return self.transformar_df(ruta)