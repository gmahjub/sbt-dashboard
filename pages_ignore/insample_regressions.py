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

# dash.register_page(__name__, path='/SignalResssions', name='Signal Regressions') # Home Page

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
fcst_vars_list = ['FcstRsweigthed', 'FcstMean', 'FcstRrr', 'FcstStd', 'FcstCount', 'FcstPercPos',
                  'HighR^2Ret', 'HighR^2Value', 'FcstPercRsfPos', 'FcstMeanPvalueTh', 'FcstStdPvalueTh',
                  'PercPosPvalueTh', 'LowPvalueRet', 'LowPvalueValue', 'FcstMeanPv05ByR^2', 'FcstStdPv05ByR^2',
                  'FcstRrrPv05ByR^2', 'FcstPercPosPv05ByR^2', 'FcstRsWeightedPv05ByR^2', 'FcstMeanPv10ByR^2',
                  'FcstStdPv10ByR^2', 'FcstRrrPv10ByR^2', 'FcstPercPosPv10ByR^2', 'FcstRsWeightedPv10ByR^2']
bq_values_list = [-1,0,1]
bl_values_list = [-1,0,1]
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
            dcc.Dropdown(bq_values_list,
                         'DYTC Model',
                         id='bq_select_dropdown'
            ),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=4,class_name=('mt-4')),
        dbc.Col(
            dcc.Dropdown(bl_values_list,
                         'DYTC Model',
                         id='bl_select_dropdown'
            ),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=4,class_name=('mt-4')),
        dbc.Col(
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
    ]),
    dbc.Row([
        dbc.Col(
            dcc.RangeSlider(
                id='range-slider', min=0, max=0.99, step=0.09, marks={0.0: 'min', 0.99: 'max'}, value=[0.0, 0.09]
            ),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
    ]),
    dbc.Row([
        dbc.Col(
            dcc.Graph(id='fcst_var_scat_plot'),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
        dbc.Col(
            dcc.Graph(figure=data_viz.scat_rate_vol(df_ms,'Rate Vol (AB)')),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
    ]),
],
    className="dbc", fluid=True)



@callback(
    Output(component_id="fcst_var_scat_plot", component_property="figure"),
    Input(component_id="range-slider", component_property="range_slider_value"))
def update_range_slider(range_slider_value):
    return data_viz.scat_fcst_var(df_ms, fcst_vars_list, range_slider_value, )


#@callback(
#    Output(component_id="rate_vol_notesObs_weblink", component_property="src"))
#def get_rate_vol_notes_iframe():
#    return rate_vol_notesObs[0]['webViewLink']

