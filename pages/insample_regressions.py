####################################
# IMPORTS
####################################

import pandas as pd
import datetime

import dash
from dash import dcc, html, callback
from dash.dependencies import Input,Output
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
from dash.dash_table import FormatTemplate
from pages import get_data
from pages import data_viz
import numpy as np

gsheets_green = '#b7e1cd'
neon_green = '#39FF14'
lime_green = "#32CD32"

dash.register_page(__name__, path='/SignalResssions', name='Signal Regressions') # Home Page

percentage = FormatTemplate.percentage(2)
data_type_dict = {'Expected Return': ('numeric', percentage),
                  'Hit Rate': ('numeric', percentage),
                  'Risk': ('numeric', percentage),
                  'Reward': ('numeric', percentage),
                  'Bias': ('text', None),
                  'DYTC Units': ('numeric', None),
                  'Within Risk @ Extrema?': ('numeric', None),
                  'Within Risk @ Settle?': ('numeric', None),
                  'Date': ('datetime', None),
                  'Actual Return': ('numeric', percentage),
                  'Day Low Return': ('numeric', percentage),
                  'Day High Return': ('numeric', percentage),
                  'ES CoD': ('numeric', None),
                  'Model CoD': ('numeric', None),
                  'Model Total': ('numeric', None),
                  'Long Always': ('numeric', None),
                  'Rate Vol (AC)': ('numeric', None),
                  'Rate Vol (AB)': ('numeric', None)
                  }

####################################
# Load data & dfs
####################################
#df_ms, update_time = get_data.get_model_signal_data('Prod-Dashboard Data', sheet_name='RealTime')
df_ms, update_time = get_data.get_input_regression_data('hist_scen_analysis_out_LAB_fixed_total',
                                                        sheet_name='hist_scen_analysis_out_LAB_fixed_total')
load_figure_template("lux")
hist_scen_data_update_time = update_time
####################################
# Page layout
####################################
as_of = html.Em(children=f'Data as of {hist_scen_data_update_time}', className=('text-center'))

layout = dbc.Container([
    dbc.Row(as_of,class_name=('mb-4')),
    dbc.Row([
            dbc.Col(
                dcc.Graph(figure=data_viz.rate_volatility_line_chart(df_ms)),
                xs=12,sm=12,md=12,lg=12,xl=12,xxl=12,class_name=('mt-4')),
    ]),
    dbc.Row([
        dbc.Col(
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
        dbc.Col(
            dcc.RangeSlider(
                id='range-slider', min=-120, max=120, step=1, marks={-120: 'min', 120: 'max'}, value=[0.5, 2]
            ),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
    ]),
    dbc.Row([
        dbc.Col(
            dcc.Graph(figure=data_viz.scat_rate_vol(df_ms, 'Rate Vol (AC)')),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
        dbc.Col(
            dcc.Graph(figure=data_viz.scat_rate_vol(df_ms,'Rate Vol (AB)')),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
    ]),
    dbc.Row([
        dbc.Col(
            dcc.Graph(figure=data_viz.bar_plot_rate_vol_binning(df_ms, np.sum)),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
        dbc.Col(
            dcc.Graph(figure=data_viz.bar_plot_rate_vol_binning(df_ms, np.std)),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
    ]),
    dbc.Row([
        dbc.Col(
            dcc.Graph(figure=data_viz.bar_plot_rate_vol_binning(df_ms, np.mean)),
            xs=12, sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
        #dbc.Col(html.Iframe(src=rate_vol_notesObs[0]['webViewLink'],
        #                    style={"height": "533px", "width": "100%"}),
        #    xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4'))
    ]),

    dbc.Row([
        dbc.Col(
            dcc.Graph(figure=data_viz.bar_plot_rate_vol_binning(df_ms, np.sum, 'Rate Vol (AB)')),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
        dbc.Col(
            dcc.Graph(figure=data_viz.bar_plot_rate_vol_binning(df_ms, np.std, 'Rate Vol (AB)')),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
    ]),

    dbc.Row([
        dbc.Col(
            dcc.Graph(figure=data_viz.bar_plot_rate_vol_binning(df_ms, np.mean, 'Rate Vol (AB)')),
            xs=12, sm=12,md=12,lg=12,xl=12,xxl=4,class_name=('mt-4')),
        #dbc.Col(html.Iframe(id='rate_vol_ab_notesObs_weblink',
        #                    style={"height": "533px", "width": "50%"}),
        #    xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4'))
    ]),
],
                           fluid=True,
                           className="dbc")


@callback(
    Output(component_id="rate_volatility_graph", component_property="figure"),
    Input(component_id="range-slider", component_property="value"))
def update_range_slider(slider_range):
    return data_viz.scat_rate_vol(df_ms, x_var='Rate Vol (AC)', y_var='Rate Vol (AB)', color_var='Market Direction',
                                  size_var="Abs Day Return", title='Rate Vol Summary Scatter')


#@callback(
#    Output(component_id="rate_vol_notesObs_weblink", component_property="src"))
#def get_rate_vol_notes_iframe():
#    return rate_vol_notesObs[0]['webViewLink']

