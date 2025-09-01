####################################
# IMPORTS
####################################
import dash
from dash import html, Dash
import dash_bootstrap_components as dbc
# import dash_core_components as dcc
from dash import dcc
from dash_bootstrap_templates import load_figure_template
from flask import Flask
from flask_restful import Resource, Api


class HealthCheck(Resource):
    def get(self):
        return {'up': 'OK'}


server = Flask('qfs_dash')
app = Dash(server=server)
api = Api(server)
api.add_resource(HealthCheck, '/health')

skip_pages = ['pages/Equities.py', 'pages/scenario_display.py', 'pages/insample_regressions.py']

from glob import glob

# Include only .py files that don't start with an underscore (e.g., _hidden_page.py)
my_pages = [f for f in glob('pages/**/*.py', recursive=True) if f not in skip_pages]


####################################
# INIT APP
####################################
dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates@V1.0.2/dbc.min.css"
app = dash.Dash(__name__,
                external_stylesheets=[dbc.themes.LUX, dbc_css],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width,initial-scale=1.0'}],
                use_pages=my_pages
                )
server = app.server

####################################
# SELECT TEMPLATE for the APP
####################################
# loads the template and sets it as the default
load_figure_template("lux")

####################################
# Main Page layout
####################################

title = html.H1(children="QFS Dashboard",
                className='text-center mt-4',
                style={'fontSize': 36})

app.layout = html.Div(children=[
    dcc.Interval(id='interval-component', interval=30*1000, n_intervals=0),
    dcc.Store(id='intermediate-value'),
    dbc.Row(title),
    dbc.Row([html.Div(id='button',
                      children=[dbc.Button(page['name'], href=page['path'])
                                for page in dash.page_registry.values() if page['name'] != 'Equities'
                                # for page in dash.page_registry.values() if page['name'] not in skip_pages
                                ],
                      className='text-center mt-4 mb-4', style={'fontSize': 20})
             ]),
    # Content page
    dbc.Spinner(
        dash.page_container,
        fullscreen=True,
        show_initially=True,
        #delay_hide=600,
        delay_show=1000,
        type='border',
        spinner_style={"width": "3rem", "height": "3rem"})

])

#############################################################################

####################################
# RUN the app
####################################
if __name__ == '__main__':
    server = app.server
    # setting this to debug = False as we are loading to GCP
    # app.run_server(debug=True)
    app.run(debug=True, host="0.0.0.0", port=8050)
