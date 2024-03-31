from acompañamientos import TablaAcompañamientos
from arriendos import TablaArriendos
from clientes import TablaClientes
from costos import TablaCostos
from extras import TablaExtras
from ingresos import TablaIngresos
from insumos import TablaInsumos
from precios import TablaPrecios
from productos import TablaProductos
from proveedores import TablaProveedores

import pandas as pd
import os
from dotenv import load_dotenv

class Transformar:
    def __init__(self) -> None:
        load_dotenv()
        self.ruta_almuerzos = os.getenv('RUTA_ALMUERZOS')
        self.ruta_otros = os.getenv('RUTA_OTROS')
        self.ruta_principal = os.getenv('RUTA_PRINCIPALES')
        self.ruta_proveedores = os.getenv('RUTA_PROVEEDORES_SIN_PROCESAR')
        self.ruta_arriendos = os.getenv('RUTA_SUBSIDIO')
        self.ruta_insumos = os.getenv('RUTA_MATERIAL')
        self.ruta_compras1 = os.getenv('RUTA_COMPRAS1')
        self.ruta_compras2 = os.getenv('RUTA_COMPRAS2')
        self.ruta_compras3 = os.getenv('RUTA_COMPRAS3')
    
    def procesar_datos(self):
        clientes = TablaClientes()
        clientes = clientes.procesar_clientes(self.ruta_almuerzos, self.ruta_otros)
        print('Tabla clientes transformada con éxito')
        
        productos = TablaProductos()
        productos = productos.procesar_productos(self.ruta_principal, self.ruta_almuerzos, self.ruta_otros)
        print('Tabla productos transformada con éxito')

        acompañamientos = TablaAcompañamientos()
        acompañamientos = acompañamientos.procesar_acompañamientos(self.ruta_almuerzos, self.ruta_otros)
        print('Tabla acompañamientos transformada con éxito')

        extras = TablaExtras()
        extras = extras.procesar_extras(self.ruta_almuerzos, self.ruta_otros)
        print('Tabla extras transformada con éxito')

        ingresos = TablaIngresos()
        ingresos = ingresos.procesar_ingresos(self.ruta_almuerzos, self.ruta_otros, clientes, productos, acompañamientos, extras)
        print('Tabla ingresos transformada con éxito')

        proveedores = TablaProveedores()
        proveedores = proveedores.procesar_proveedores(self.ruta_proveedores)
        print('Tabla proveedores transformada con éxito')

        precios = TablaPrecios()
        precios = precios.procesar_precios(self.ruta_principal)
        print('Tabla precios transformada con éxito')

        arriendos = TablaArriendos()
        arriendos = arriendos.procesar_arriendos(self.ruta_arriendos)
        print('Tabla arriendos transformada con éxito')

        insumos = TablaInsumos()
        insumos = insumos.procesar_insumos(self.ruta_insumos)
        print('Tabla insumos transformada con éxito')

        costos = TablaCostos()
        costos = costos.procesar_costos(self.ruta_compras1, self.ruta_compras2, self.ruta_compras3, proveedores, insumos)
        print('Tabla costos transformada con éxito')

        return [acompañamientos, arriendos, clientes, costos, extras, ingresos, insumos, precios, productos, proveedores]


