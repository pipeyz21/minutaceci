from tabla_base import TablaBase

class TablaPrincipales(TablaBase):
    def transformar_df(self, ruta):
        return super().transformar_df(ruta)
    def procesar_principales(self,ruta):
        return self.transformar_df(ruta)