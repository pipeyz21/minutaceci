from dash import Dash, html, dash_table, dcc, callback, Output, Input
from data import ventas, otros
import plotly.express as px

app = Dash(__name__)

style_path = 'styles.css'

figAlmuerzos = px.bar(ventas, x='Fecha', y='Total')
figOtros = px.bar(otros, x='Fecha', y='Total')

app.layout = html.Div([
    html.Link(
        rel='stylesheet',
        href='/assets/estilos.css'  # Ruta relativa al directorio 'assets' donde se encuentra tu archivo CSS
    ),
    html.H1(
        children='MinutaCeci Dashboard - Analisis Exploratorio', 
        style={
            'textAlign' : 'center'
        }),
    html.Hr(),
    html.P(children='Almuerzos diarios', className='parrafo'),
    dcc.Graph(figure=figAlmuerzos, id='figAlmuerzos'),
    dash_table.DataTable(data=ventas.to_dict('records'), page_size=10),
    html.Hr(),
    html.P(children='Otros ingresos', className='parrafo'),
    dcc.Graph(figure=figOtros, id='figOtros'),
    dash_table.DataTable(data=otros.to_dict('records'), page_size=10),
])

if __name__ == '__main__':
    app.run(debug=True)