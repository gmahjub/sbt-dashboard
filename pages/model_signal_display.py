####################################
# IMPORTS
####################################

import dash
from dash import dcc, html, dash_table, callback, ctx
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
from dash.dash_table import FormatTemplate
from pages import get_data
from pages import data_viz
from datetime import date, timedelta, datetime
import pandas as pd

gsheets_green = '#b7e1cd'
neon_green = '#39FF14'
lime_green = "#32CD32"

dash.register_page(__name__,path='/',name='Model Main') # Home Page

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
                  'DYTC Model CoD': ('numeric', None),
                  'DYTC Model Total': ('numeric', None),
                  'Long Always': ('numeric', None),
                  'Fundamental Model Total': ('numeric', None),
                  '5-D Long Model Total': ('numeric', None),
                  '5-D Long Cumm Model Total': ('numeric', None),
                  'SPY(CM) Total, ES Pts': ('numeric', None),
                  'SPY(AH) Total, ES Pts': ('numeric', None),
                  'Trade Plan Publish Date/Time (CST)': ('datetime', None),
                  'Integrity?': ('text', None),
                  'Trade Plan Link': ('text', None),
                  'Final Signal Link': ('text', None),
                  'Prelim Signal Link': ('text', None)
                  }


####################################
# Load data & dfs
####################################
df_ms, update_time = get_data.get_model_signal_data('Prod-Dashboard Data', sheet_name='RealTime')
earliest_date = list(df_ms.values())[1].Date.iloc[-1]
latest_date = list(df_ms.values())[1].Date.iloc[0]
# df['DOB'].dt.year.unique()
list_of_years = pd.to_datetime(list(df_ms.values())[1].Date).dt.year.unique().tolist() + ['All']
model_list_df = get_data.get_models_list('Prod-Dashboard Data', sheet_name='Model List')
accuracy_input_df = df_ms['Accuracy']
accuracy_df = get_data.create_accuracy_df(accuracy_input_df)
dytc_px_levels_df = get_data.get_dytc_px_levels(df_ms)
the_trade_plans = get_data.get_trade_plans()
final_signal_nc_str = "name contains 'Final Signal' and name contains 'SB Trades'"
prelim_signal_nc_str = "name contains 'Prelim Signal' and name contains 'SB Trades'"
the_final_signals = get_data.get_trade_plans(name_contains=final_signal_nc_str)
the_prelim_signals = get_data.get_trade_plans(name_contains=prelim_signal_nc_str)
signal_integrity_df = get_data.create_integrity_df(df_ms, the_trade_plans, the_final_signals, the_prelim_signals)
risk_event_analysis_df, rea_data_type_dict = get_data.create_re_analysis_df(df_ms)
df = get_data.get_rates()
load_figure_template("lux")
#rates_update_time = df.index[-1].strftime("%b-%Y")
rates_update_time = update_time
#rates_update_time = df.index[-1].strftime("%b-%Y")

####################################
# Page layout
####################################
as_of = html.Em(children=f'Data as of {rates_update_time}',
                className=('text-center'))

layout = dbc.Container([
    dbc.Row(as_of,class_name=('mb-4')),
    dbc.Row([
        dbc.Col(
            dcc.Dropdown(list_of_years,
                         str(update_time.year-1),
                         id='year-filter-dropdown'),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=4,class_name=('mt-4')),
        dbc.Col(
            dcc.Checklist(['Rolling Performance'],
                          ['Rolling Performance'],
                          inline=True,
                          id='rolling_performance_flag'),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=4,class_name=('mt-4')),
    ]),
    dbc.Row([
        dbc.Col(
            dcc.DatePickerRange(id='my-date-picker-range',
                                minimum_nights=5,
                                clearable=True,
                                with_portal=True,
                                start_date=earliest_date,
                                end_date=latest_date,
                                min_date_allowed=earliest_date,
                                max_date_allowed=latest_date,
                                initial_visible_month=latest_date - timedelta(days=30)),
            #html.Div(id='output-container-date-picker-range'),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
        dbc.Col(
            html.Button('Reset', id='reset-button', n_clicks=0),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=2,class_name=('mt-4')),
        dbc.Col(
            dcc.Dropdown(model_list_df['Name'].values,
                         'DYTC Model',
                         id='model_select_dropdown'
            ),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=4,class_name=('mt-4')),
    ]),

    dbc.Row([
            dbc.Col(
                dcc.Graph(id='output-container-date-picker-range'),
                #dcc.Graph(figure=data_viz.line_pnl(list(df_ms.values())[1],
                #                                   visible_list=['DYTC Model Total', 'Long Always'])),
                xs=12,sm=12,md=12,lg=12,xl=12,xxl=8,class_name=('mt-4')),
            dbc.Col(
                dcc.Graph(id='pnl_histogram'),
                xs=12,sm=12,md=12,lg=12,xl=12,xxl=4,className=('mt-4')),
    ]),
    dbc.Row([
        dbc.Col(
            dcc.Graph(figure=data_viz.bar_plot_accuracy_stats(accuracy_df)),
            xs=12,sm=12,md=12,lg=12,xl=6,xxl=6,class_name=('mt-4')),
        dbc.Col(
            dcc.Graph(figure=data_viz.performance_tree(accuracy_df)),
            xs=12,sm=12,md=12,lg=12,xl=6,xxl=6,class_name=('mt-4')),
    ]),

    dbc.Row([
        dbc.Label(list(df_ms.keys())[0], style={'fontSize': '20px', 'textAlign': 'left'}),
        dbc.Col(dash_table.DataTable(list(df_ms.values())[0].to_dict('records'), \
                                     [{"name": i, "id": i, "type": data_type_dict[i][0],
                                       "format": data_type_dict[i][1]} for i in list(df_ms.values())[0].columns],
                                     style_data_conditional=[
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = S || {{{col}}} < 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#ffcccb',
                                         } for col in list(df_ms.values())[0].columns
                                     ] + [
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = L || {{{col}}} > 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#32CD32'
                                         } for col in list(df_ms.values())[0].columns
                                     ] + [
                                         {
                                             'if': {
                                                 'column_id': c
                                             },
                                             'textAlign': 'right'
                                         } for c in ['Units', 'Expected Return', 'Risk', 'Reward', 'Hit Rate',
                                                     'Actual Return', 'Within Risk @ Extrema?', 'Within Risk @ Settle?']
                                     ] + [
                                         {
                                             'if': {
                                                 'filter_query': '{Hit Rate} > 0.60',
                                                 'column_id': 'Hit Rate'
                                             },
                                             'backgroundColor': '#32CD32'
                                         }
                                     ] + [
                                         {
                                             'if': {
                                                 'filter_query': '{Hit Rate} < 0.60',
                                                 'column_id': 'Hit Rate'
                                             },
                                             'backgroundColor': 'white'
                                         }
                                     ],
                                     style_cell={'textAlign': 'left', 'fontSize': '12px'},
                                     style_header={
                                         'backgroundColor': '#EBECF0',
                                         'fontWeight': 'bold'
                                     },
                                     page_size=10,
                                     sort_action="native",
                                     sort_mode="multi",
                                     style_as_list_view=True)),
    ]),
    #dbc.Row('',class_name=('mb-4')),
    dbc.Row([
        dbc.Label("SBT Trade Plan", style={'fontSize': '20px', 'textAlign': 'left'}),
        dbc.Label("Verify Signal Integrity", style={'fontSize': '20px', 'textAlign': 'right'}),
        dbc.Col(
            dcc.Dropdown([x['name'] for x in the_trade_plans],
                         the_trade_plans[0]['name'],
                         id='trade_plan_select_dropdown'
            ),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=4,class_name=('mt-4')),

        dbc.Col(
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=8,class_name=('mt-4')),
    ]),
    dbc.Row([
        dbc.Col(html.Iframe(id='trade_plan_weblink',
                            style={"height": "533px", "width": "100%"}),
                xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')
                ),
        #dbc.Col(html.Iframe(src="https://docs.google.com/document/d/e/"
        #                        "2PACX-1vQarfihQRaUt6qoCRI3jXsE36NCisBSqY6p"
        #                        "VAVDYLAVkcRzt7scu9M_JWHNhpmaFHG-UWAQnW19zOfj/pub?embedded=true",
        #                    style={"height": "533px", "width": "50%"}),
        #        xs=12,sm=12,md=12,lg=12,xl=12,xxl=12,class_name=('mt-4'))
        dbc.Col(dash_table.DataTable(signal_integrity_df.to_dict('records'),\
                                     [{"name": i, "id": i, "type": data_type_dict[i][0], "format": data_type_dict[i][1]
                                       } if 'Link' not in i else {"name": i, "id": i, "type": data_type_dict[i][0],
                                                                  "format": data_type_dict[i][1],
                                                                  "presentation": "markdown"}
                                      for i in signal_integrity_df.columns],
                                     style_cell={'textAlign': 'left', 'fontSize': '12px'},
                                     style_header={
                                         'backgroundColor': '#EBECF0',
                                         'fontWeight': 'bold'
                                     },
                                     page_size=13,
                                     style_data_conditional=[{
                                             'if': {
                                                 'filter_query': '{{{col}}} = S || {{{col}}} < 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#ffcccb',
                                         } for col in signal_integrity_df.columns
                                     ] + [
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = L || {{{col}}} > 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#32CD32'
                                         } for col in signal_integrity_df.columns
                                     ] + [
                                         {
                                             'if': {
                                                 'column_id': col
                                             },
                                             'color': 'blue',
                                             'fontWeight': 'bold'
                                         } for col in ['Trade Plan Link', 'Final Signal Link', 'Prelim Signal Link']
                                     ] + [
                                         {
                                             'if': {
                                                 'filter_query': '{Integrity?} = Verified',
                                                 'column_id': 'Integrity?'
                                             },
                                             'color': 'blue'
                                         }
                                     ] + [
                                         {
                                             'if': {
                                                 'filter_query': '{Integrity?} = Unverified',
                                                 'column_id': 'Integrity?'
                                             },
                                             'color': 'red'
                                         }
                                     ]),
                xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4'))
    ]),
    dbc.Row('',class_name=('mb-4')),
    dbc.Row([
        dbc.Label(list(df_ms.keys())[7], style={'fontSize': '20px', 'textAlign': 'left'}),
        dbc.Col(dash_table.DataTable(list(df_ms.values())[7].to_dict('records'),\
                                     [{"name": i, "id": i, "type": data_type_dict[i][0], "format": data_type_dict[i][1]
                                       } for i in list(df_ms.values())[7].columns],
                                     style_cell={'textAlign': 'left', 'fontSize': '12px'},
                                     style_data_conditional=[
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = S || {{{col}}} < 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#ffcccb',
                                         } for col in list(df_ms.values())[7].columns
                                     ] + [
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = L || {{{col}}} > 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#32CD32'
                                         } for col in list(df_ms.values())[7].columns
                                     ] + [
                                         {
                                             'if': {'column_id': c},
                                             'textAlign': 'right'
                                         } for c in ['Units', 'Expected Return', 'Risk', 'Reward', 'Hit Rate']
                                     ] + [
                                         {
                                             'if': {'column_id': c},
                                             'fontWeight': 'bold'
                                         } for c in ['DYTC Model Total', 'Long Always']
                                     ],
                                     style_header={
                                         'backgroundColor': 'light grey',
                                         'fontWeight': 'bold'
                                     },
                                     page_size=10,
                                     sort_action="native",
                                     sort_mode="multi",
                                     style_as_list_view=True)),

    ]),
    dbc.Row('',class_name=('mb-4')),
    dbc.Row([
        dbc.Label('Risk Event Analysis', style={'fontSize': '20px', 'textAlign': 'left'}),
        dbc.Col(dash_table.DataTable(risk_event_analysis_df.to_dict('records'),\
                                     [{"name": i, "id": i, "type": rea_data_type_dict[i][0],
                                       "format": rea_data_type_dict[i][1]} for i in risk_event_analysis_df.columns],
                                     style_cell={'textAlign': 'left', 'fontSize': '12px'},
                                     style_data_conditional=[
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = S || {{{col}}} < 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#ffcccb',
                                         } for col in risk_event_analysis_df.columns
                                     ] + [
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = L || {{{col}}} > 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#32CD32'
                                         } for col in risk_event_analysis_df.columns
                                     ] + [
                                         {
                                             'if': {'column_id': c},
                                             'textAlign': 'right'
                                         } for c in ['Units', 'Expected Return', 'Risk', 'Reward', 'Hit Rate']
                                     ] + [
                                         {
                                             'if': {'column_id': c},
                                             'fontWeight': 'bold'
                                         } for c in ['DYTC Model Total', 'Long Always']
                                     ],
                                     style_header={
                                         'backgroundColor': 'light grey',
                                         'fontWeight': 'bold'
                                     },
                                     page_size=20,
                                     sort_action="native",
                                     sort_mode="multi",
                                     style_as_list_view=True)),

    ])
], fluid=True, className="dbc")


@callback(
    Output(component_id="pnl_histogram", component_property="figure"),
    Input(component_id="model_select_dropdown", component_property="value"))
def update_model_pnl_data(model_select_name):
    return data_viz.pnl_histogram(df_ms, model_list_df, model_select_name)


@callback(
    Output(component_id="trade_plan_weblink", component_property="src"),
    Input(component_id="trade_plan_select_dropdown", component_property="value"))
def update_trade_plan_iframe(trade_plan_name):
    for x in the_trade_plans:
        if x['name'] == trade_plan_name:
            return x['webViewLink']


@callback(
    Output(component_id='output-container-date-picker-range', component_property='figure'), # or children
    Input(component_id='my-date-picker-range', component_property='start_date'),
    Input(component_id='my-date-picker-range', component_property='end_date'),
    Input(component_id='reset-button', component_property='n_clicks'),
    Input(component_id='year-filter-dropdown', component_property='value'),
    Input(component_id='rolling_performance_flag', component_property='value')
)
def update_pnl_line_graph(start_date, end_date, reset_button, year_filter, rolling_perf_flag):
    triggered_id = ctx.triggered_id
    if triggered_id == 'reset-button':
        start_date = str(earliest_date)
        end_date = str(latest_date)
    if start_date is not None:
        start_date_object = date.fromisoformat(start_date)
    else:
        start_date_object = None
    if end_date is not None:
        end_date_object = date.fromisoformat(end_date)
    else:
        end_date_object = None
    return data_viz.line_pnl(list(df_ms.values())[1],
                             visible_list=['DYTC Model Total', 'Long Always'],
                             start_date=start_date_object,
                             end_date=end_date_object,
                             year_filter=year_filter,
                             rolling_perf_flag=rolling_perf_flag)
