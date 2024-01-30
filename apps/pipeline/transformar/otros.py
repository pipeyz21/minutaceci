from tabla_base import TablaBase

class TablaOtros(TablaBase):
    def transformar_df(self, ruta):
        df = super().transformar_df(ruta)
        return df
    
    def procesar_otros(self, ruta):       
        return self.transformar_df(ruta)