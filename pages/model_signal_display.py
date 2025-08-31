####################################
# IMPORTS
####################################

import dash, os, json, re
from dash import dcc, html, dash_table, callback, ctx
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import dash_daq as daq
from dash_bootstrap_templates import load_figure_template
from dash.dash_table import FormatTemplate
from pages import get_data
from pages import data_viz
from datetime import date, timedelta, datetime
import pandas as pd

default_acct_num = os.getenv("DEFAULT_BROKER_ACCT_NUM")
if default_acct_num is None:
    default_acct_num = "DU9822977"
gsheets_green = '#b7e1cd'
neon_green = '#39FF14'
lime_green = "#32CD32"

today_dt_date = datetime.now()-timedelta(days=2)
today_str_date = today_dt_date.strftime("%Y%m%d")

trade_tracker_categories = ['USEquity', 'G10Currency', 'Metals', 'UsTreasuries', 'Energy']

dash.register_page(__name__, path='/', name='Model Main') # Home Page

percentage = FormatTemplate.percentage(2)
data_type_dict = {'Timestamp': ('datetime', None),
                  'PositionTimestamp': ('datetime', None),
                  'PnlTimestamp': ('datetime', None),
                  'Expected Return': ('numeric', percentage),
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
                  'Prelim Signal Link': ('text', None),
                  'ConSym': ('text', None),
                  'OrderType': ('text', None),
                  'OrderAction': ('text', None),
                  'OrderQuantity': ('numeric', None),
                  'OrderStatus': ('text', None),
                  'Symbol': ('text', None),
                  'ExpirationMonth': ('text', None),
                  'Time': ('datetime', None),
                  'ExecId': ('text', None),
                  'Exchange': ('text', None),
                  'Side': ('text', None),
                  'NumContracts': ('numeric', None),
                  'Price': ('numeric', None),
                  'CumQty': ('numeric', None),
                  'AvgPrice': ('numeric', None),
                  }


####################################
# Load data & dfs
####################################
# df_ms, update_time = get_data.get_model_signal_data('Prod-Dashboard Data', sheet_name='RealTime')
fills_df, fills_update_time = get_data.get_any_data_type_df(str_date=today_str_date,
                                                            dt_date=today_dt_date,
                                                            data_type='fills',
                                                            acct_num=default_acct_num)
if fills_df is None and fills_update_time is None:
    # we don't have any fills for the str_date that was input
    fills_df = pd.DataFrame()
to_display_fills_df = fills_df[['Symbol', 'ExpirationMonth', 'Time', 'ExecId', 'Exchange', 'Side', 'NumContracts',
                                'Price', 'AvgPrice', 'CumQty']].copy()
open_orders_df, open_orders_update_time = get_data.get_any_data_type_df(str_date=today_str_date,
                                                                        data_type='open_orders',
                                                                        acct_num=default_acct_num)
to_display_open_orders_df = open_orders_df[['ConSym', 'OrderType', 'OrderAction', 'OrderQuantity', 'OrderStatus']].copy()
position_pnl_df, position_pnl_update_time = get_data.get_any_data_type_df(str_date=today_str_date,
                                                                          data_type='position_pnl',
                                                                          acct_num=default_acct_num)
position_df, position_update_time = get_data.get_any_data_type_df(str_date=today_str_date, acct_num=default_acct_num)
avg_cost_df, avg_cost_update_time = get_data.get_any_data_type_df(str_date=today_str_date, data_type='avgcost',
                                                                  acct_num=default_acct_num)
pnl_tracker_df, pnl_tracker_update_time = get_data.get_any_data_type_df(str_date=today_str_date,
                                                                        data_type='pnltracker',
                                                                        acct_num=default_acct_num)
daily_pnl_timeseries_df, unrealized_pnl_timeseries_df, position_pnl_df = (
    get_data.create_daily_timeseries(position_df, avg_cost_df, position_pnl_df, transposed=False))
curr_avail_pos_pnl_con_list = daily_pnl_timeseries_df.columns.get_level_values(0).unique()
total_position_df = get_data.create_total_position_df(position_df, avg_cost_df, position_pnl_df, transposed=False)
total_position_df = total_position_df.reset_index().rename(columns={'index': 'Contract'})
available_position_dates = get_data.get_file_type_dates(data_type='positions', acct_num=default_acct_num)
available_pnltracker_dates = get_data.get_file_type_dates(data_type='pnltracker', acct_num=default_acct_num)
trade_tracker_html_doc_dict = get_data.get_trade_tracker_html_docs()
# get the latest set of the trade tracker hmtl docs, which means get the last n elements, where n is length of cats
tthdd_keys, tthdd_values = zip(*trade_tracker_html_doc_dict.items())
trade_tracker_html_doc_dict =\
    dict(zip(tthdd_keys[len(trade_tracker_categories)*-1:], tthdd_values[len(trade_tracker_categories)*-1:]))
earliest_date = available_position_dates[0]
latest_date = available_position_dates[-1]
# df['DOB'].dt.year.unique()
# list_of_years = pd.to_datetime(list(df_ms.values())[1].Date).dt.year.unique().tolist() + ['All']
# model_list_df = get_data.get_models_list('Prod-Dashboard Data', sheet_name='Model List')
# accuracy_input_df = df_ms['Accuracy']
# accuracy_df = get_data.create_accuracy_df(accuracy_input_df)
# dytc_px_levels_df = get_data.get_dytc_px_levels(df_ms)
# the_trade_plans = get_data.get_trade_plans()
final_signal_nc_str = "name contains 'Final Signal' and name contains 'SB Trades'"
prelim_signal_nc_str = "name contains 'Prelim Signal' and name contains 'SB Trades'"
# the_final_signals = get_data.get_trade_plans(name_contains=final_signal_nc_str)
# the_prelim_signals = get_data.get_trade_plans(name_contains=prelim_signal_nc_str)
# signal_integrity_df = get_data.create_integrity_df(df_ms, the_trade_plans, the_final_signals, the_prelim_signals)
# risk_event_analysis_df, rea_data_type_dict = get_data.create_re_analysis_df(df_ms)
df = get_data.get_rates()
load_figure_template("lux")
# rates_update_time = df.index[-1].strftime("%b-%Y")
rates_update_time = position_update_time
# rates_update_time = df.index[-1].strftime("%b-%Y")

####################################
# Page layout
####################################
as_of = html.Em(children=f'Data as of {rates_update_time}',
                className=('text-center'))

led_display = daq.LEDDisplay(
    id="daily-pnl-display",
    value="0.00",
    label={
        "label": "Daily P&L (since last settle)",
        "style": {"font-size": "1.6rem", "text-align": "center"},
    },
    backgroundColor="black",
    color="red",
    labelPosition="bottom",
    size=50,
)

layout = dbc.Container([
    dbc.Row(as_of,class_name=('mb-4')),
    dbc.Row([
        dbc.Col(
            dcc.Dropdown(available_pnltracker_dates,
                         today_str_date,
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
                                initial_visible_month=datetime.strptime(latest_date, "%Y%m%d") - timedelta(days=30)),
            #html.Div(id='output-container-date-picker-range'),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
        dbc.Col(
            html.Button('Reset', id='reset-button', n_clicks=0),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=2,class_name=('mt-4')),
        dbc.Col(
            dcc.Dropdown(curr_avail_pos_pnl_con_list,
                         curr_avail_pos_pnl_con_list[0],
                         id='pos_pnl_select_dropdown'
            ),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=4,class_name=('mt-4')),
    ]),
    dbc.Row(
        [
            dbc.Col([led_display], lg=6, style=dict(textAlign="center"))],
        justify="center",
        className="mt-4",
    ),
    dbc.Row([
            dbc.Col(
                # dcc.Graph(id='output-container-date-picker-range'),
                dcc.Graph(figure=data_viz.line_pnl(pnl_tracker_df,
                                                   visible_list=['DailyPnL'])),
                xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
            dbc.Col(
                #dcc.Graph(figure=data_viz.line_pnl(daily_pnl_timeseries_df, visible_list=['DailyPnL'])),
                # dcc.Graph(figure=data_viz.line_pnl(daily_pnl_timeseries_df['6JU5'], visible_list=['DailyPnL'])),
                dcc.Graph(id='contract_pnl_figure'),
                # dcc.Graph(id='pnl_histogram'),
                xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,className=('mt-4')),

    ]),
    # dbc.Row([
    #     dbc.Col(
    #         dcc.Graph(figure=data_viz.bar_plot_accuracy_stats(accuracy_df)),
    #         xs=12,sm=12,md=12,lg=12,xl=6,xxl=6,class_name=('mt-4')),
    #     dbc.Col(
    #         dcc.Graph(figure=data_viz.performance_tree(accuracy_df)),
    #         xs=12,sm=12,md=12,lg=12,xl=6,xxl=6,class_name=('mt-4')),
    # ]),

    dbc.Row([
        # dbc.Label(list(total_position_df.keys())[0], style={'fontSize': '20px', 'textAlign': 'left'}),
        dbc.Col(dash_table.DataTable(total_position_df.to_dict('records'), \
                                     [{"name": i, "id": i, "type": data_type_dict.setdefault(i, ('numeric', None))[0],
                                       "format": data_type_dict.setdefault(i, ('numeric', None))[1]}
                                      for i in total_position_df.columns],
                                     style_data_conditional=[
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = S || {{{col}}} < 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#ffcccb',
                                         } for col in total_position_df.columns
                                     ] + [
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = L || {{{col}}} > 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#32CD32'
                                         } for col in total_position_df.columns
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
        dbc.Label("QFS Trade Tracker", style={'fontSize': '20px', 'textAlign': 'left'}),
        # dbc.Label("7-Day", style={'fontSize': '20px', 'textAlign': 'right'}),
        dbc.Col(
            dcc.Dropdown([x for x, y in trade_tracker_html_doc_dict.items()],
                         f'QFS_USEquity_TradeTrackerApp_{today_str_date}.html',
                         id='trade_plan_select_dropdown'
            ),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=4,class_name=('mt-4')),

        dbc.Col(
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=8,class_name=('mt-4')),
    ]),
    dbc.Row([
        dbc.Col(html.Iframe(#src="https://sbt-public-share.s3.amazonaws.com/QFS_Energy_TradeTrackerApp_20250813.html?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQQABDV7WCMEZDXMK%2F20250826%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20250826T173436Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=3f190e19596d19d2e03b83d4b195d08783426f6672674111cf51e35e0db68951&Content-Type=text/html",
                            # src="https://sbt-public-share.s3.us-east-2.amazonaws.com/QFS_USEquity_TradeTrackerApp_20250826.html",
                            id='trade_plan_weblink',
                            style={"height": "750px", "width": "100%"}),
                xs=12,sm=12,md=12,lg=12,xl=12,xxl=12,class_name=('mt-4')
                ),
        #dbc.Col(html.Iframe(id='trade_plan_weblink',
        #                    style={"height": "533px", "width": "100%"}),
        #        xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')
        #        ),
        #dbc.Col(html.Iframe(src="https://docs.google.com/document/d/e/"
        #                        "2PACX-1vQarfihQRaUt6qoCRI3jXsE36NCisBSqY6p"
        #                        "VAVDYLAVkcRzt7scu9M_JWHNhpmaFHG-UWAQnW19zOfj/pub?embedded=true",
        #                    style={"height": "533px", "width": "50%"}),
        #        xs=12,sm=12,md=12,lg=12,xl=12,xxl=12,class_name=('mt-4'))
        # dbc.Col(dash_table.DataTable(total_position_df.to_dict('records'),\
        #                              [{"name": i, "id": i, "type": data_type_dict[i][0], "format": data_type_dict[i][1]
        #                                } if 'Link' not in i else {"name": i, "id": i, "type": data_type_dict[i][0],
        #                                                           "format": data_type_dict[i][1],
        #                                                           "presentation": "markdown"}
        #                               for i in total_position_df.columns],
        #                              style_cell={'textAlign': 'left', 'fontSize': '12px'},
        #                              style_header={
        #                                  'backgroundColor': '#EBECF0',
        #                                  'fontWeight': 'bold'
        #                              },
        #                              page_size=13,
        #                              style_data_conditional=[{
        #                                      'if': {
        #                                          'filter_query': '{{{col}}} = S || {{{col}}} < 0.0'.format(col=col),
        #                                          'column_id': col
        #                                      },
        #                                      'backgroundColor': '#ffcccb',
        #                                  } for col in total_position_df.columns
        #                              ] + [
        #                                  {
        #                                      'if': {
        #                                          'filter_query': '{{{col}}} = L || {{{col}}} > 0.0'.format(col=col),
        #                                          'column_id': col
        #                                      },
        #                                      'backgroundColor': '#32CD32'
        #                                  } for col in total_position_df.columns
        #                              ] + [
        #                                  {
        #                                      'if': {
        #                                          'column_id': col
        #                                      },
        #                                      'color': 'blue',
        #                                      'fontWeight': 'bold'
        #                                  } for col in ['Trade Plan Link', 'Final Signal Link', 'Prelim Signal Link']
        #                              ] + [
        #                                  {
        #                                      'if': {
        #                                          'filter_query': '{Integrity?} = Verified',
        #                                          'column_id': 'Integrity?'
        #                                      },
        #                                      'color': 'blue'
        #                                  }
        #                              ] + [
        #                                  {
        #                                      'if': {
        #                                          'filter_query': '{Integrity?} = Unverified',
        #                                          'column_id': 'Integrity?'
        #                                      },
        #                                      'color': 'red'
        #                                  }
        #                              ]),
        #         xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4'))
    ]),
    dbc.Row('',class_name=('mb-4')),
    dbc.Row([
        dbc.Label("QFS Contract Sizing & Signals", style={'fontSize': '20px', 'textAlign': 'left'}),
        #dbc.Label("Max Allowed", style={'fontSize': '20px', 'textAlign': 'right'}),
        #dbc.Col(
        #    dcc.Dropdown([x for x, y in trade_tracker_html_doc_dict.items()],
        #                 f'QFS_USEquity_TradeTrackerApp_{today_str_date}.html',
        #                 id='trade_plan_select_dropdown'
        #    ),
        #    xs=12,sm=12,md=12,lg=12,xl=12,xxl=4,class_name=('mt-4')),
        #
        #dbc.Col(
        #    xs=12,sm=12,md=12,lg=12,xl=12,xxl=8,class_name=('mt-4')),
    ]),
    dbc.Row([
        dbc.Col(html.Iframe(#src="https://sbt-public-share.s3.amazonaws.com/QFS_Energy_TradeTrackerApp_20250813.html?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQQABDV7WCMEZDXMK%2F20250826%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20250826T173436Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=3f190e19596d19d2e03b83d4b195d08783426f6672674111cf51e35e0db68951&Content-Type=text/html",
                            # src="https://sbt-public-share.s3.us-east-2.amazonaws.com/QFS_USEquity_TradeTrackerApp_20250826.html",
                            src="https://datawrapper.dwcdn.net/U0avf/4/",
                            #id='trade_plan_weblink',
                            style={"height": "533px", "width": "100%"}),
                xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')
                ),
        dbc.Col(html.Iframe(#src="https://sbt-public-share.s3.amazonaws.com/QFS_Energy_TradeTrackerApp_20250813.html?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAQQABDV7WCMEZDXMK%2F20250826%2Fus-east-2%2Fs3%2Faws4_request&X-Amz-Date=20250826T173436Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=3f190e19596d19d2e03b83d4b195d08783426f6672674111cf51e35e0db68951&Content-Type=text/html",
                            # src="https://sbt-public-share.s3.us-east-2.amazonaws.com/QFS_USEquity_TradeTrackerApp_20250826.html",
                            src="https://datawrapper.dwcdn.net/My1Bw/3/",
                            #id='trade_plan_weblink',
                            style={"height": "533px", "width": "100%"}),
                xs=12,sm=12,md=12,lg=12,xl=12,xxl=6,class_name=('mt-4')),
    ]),
    dbc.Row('',class_name=('mb-4')),
    dbc.Row([
        # dbc.Label("QFS Contract Sizing & Signals", style={'fontSize': '20px', 'textAlign': 'left'}),
        #dbc.Label("Max Allowed", style={'fontSize': '20px', 'textAlign': 'right'}),
        dbc.Col(
            dbc.Label("Open Orders Table", style={'fontSize': '20px', 'textAlign': 'left'}),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=4,class_name=('mt-4')),

        dbc.Col(
            dbc.Label("Daily Filled Orders", style={'fontSize': '20px', 'textAlign': 'left'}),
            xs=12,sm=12,md=12,lg=12,xl=12,xxl=8,class_name=('mt-4')),
    ]),
    dbc.Row([
        # dbc.Label("Open Orders Table", style={'fontSize': '20px', 'textAlign': 'left'}),
        dbc.Col(dash_table.DataTable(to_display_open_orders_df.to_dict('records'),
                                     [{"name": i, "id": i, "type": data_type_dict[i][0], "format": data_type_dict[i][1]
                                       } for i in to_display_open_orders_df.columns],
                                     style_cell={'textAlign': 'left', 'fontSize': '12px'},
                                     style_data_conditional=[
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = S || {{{col}}} < 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#ffcccb',
                                         } for col in to_display_open_orders_df.columns
                                     ] + [
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = L || {{{col}}} > 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#32CD32'
                                         } for col in to_display_open_orders_df.columns
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
                                     style_as_list_view=True), xs=12,sm=12,md=12,lg=12,xl=12,xxl=4,class_name=('mt-4')),
        # dbc.Label("Daily Filled Orders", style={'fontSize': '20px', 'textAlign': 'left'}),
        dbc.Col(dash_table.DataTable(to_display_fills_df.to_dict('records'),
                                     [{"name": i, "id": i, "type": data_type_dict[i][0], "format": data_type_dict[i][1]
                                       } for i in to_display_fills_df.columns],
                                     style_cell={'textAlign': 'left', 'fontSize': '12px'},
                                     style_data_conditional=[
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = S || {{{col}}} < 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#ffcccb',
                                         } for col in to_display_fills_df.columns
                                     ] + [
                                         {
                                             'if': {
                                                 'filter_query': '{{{col}}} = L || {{{col}}} > 0.0'.format(col=col),
                                                 'column_id': col
                                             },
                                             'backgroundColor': '#32CD32'
                                         } for col in to_display_fills_df.columns
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
                                     style_as_list_view=True), xs=12,sm=12,md=12,lg=12,xl=12,xxl=8,class_name=('mt-4'))
    ]),
    # dbc.Row('',class_name=('mb-4')),
    # dbc.Row([
    #     dbc.Label('Risk Event Analysis', style={'fontSize': '20px', 'textAlign': 'left'}),
    #     dbc.Col(dash_table.DataTable(position_df.to_dict('records'),\
    #                                  [{"name": i, "id": i, "type": rea_data_type_dict[i][0],
    #                                    "format": rea_data_type_dict[i][1]} for i in risk_event_analysis_df.columns],
    #                                  style_cell={'textAlign': 'left', 'fontSize': '12px'},
    #                                  style_data_conditional=[
    #                                      {
    #                                          'if': {
    #                                              'filter_query': '{{{col}}} = S || {{{col}}} < 0.0'.format(col=col),
    #                                              'column_id': col
    #                                          },
    #                                          'backgroundColor': '#ffcccb',
    #                                      } for col in risk_event_analysis_df.columns
    #                                  ] + [
    #                                      {
    #                                          'if': {
    #                                              'filter_query': '{{{col}}} = L || {{{col}}} > 0.0'.format(col=col),
    #                                              'column_id': col
    #                                          },
    #                                          'backgroundColor': '#32CD32'
    #                                      } for col in risk_event_analysis_df.columns
    #                                  ] + [
    #                                      {
    #                                          'if': {'column_id': c},
    #                                          'textAlign': 'right'
    #                                      } for c in ['Units', 'Expected Return', 'Risk', 'Reward', 'Hit Rate']
    #                                  ] + [
    #                                      {
    #                                          'if': {'column_id': c},
    #                                          'fontWeight': 'bold'
    #                                      } for c in ['DYTC Model Total', 'Long Always']
    #                                  ],
    #                                  style_header={
    #                                      'backgroundColor': 'light grey',
    #                                      'fontWeight': 'bold'
    #                                  },
    #                                  page_size=20,
    #                                  sort_action="native",
    #                                  sort_mode="multi",
    #                                  style_as_list_view=True)),
    #
    # ])
], fluid=True, className="dbc")


#@callback(
#    Output(component_id="pnl_histogram", component_property="figure"),
#    Input(component_id="model_select_dropdown", component_property="value"))
#def update_model_pnl_data(model_select_name):
#    df_ms = None
#    model_list_df = None
#    return data_viz.pnl_histogram(df_ms, model_list_df, model_select_name)


@callback(
    Output(component_id="trade_plan_weblink", component_property="src"),
    Input(component_id="trade_plan_select_dropdown", component_property="value"))
def update_trade_plan_iframe(trade_plan_name):
    for key, value in trade_tracker_html_doc_dict.items():
        if key == trade_plan_name:
            return value


@callback(
    dash.Output(component_id='contract_pnl_figure', component_property='figure'), # or children
    dash.Input(component_id='pos_pnl_select_dropdown', component_property='value'))
def update_contract_pnl_graph(the_con):
    # triggered_id = ctx.triggered_id
    # if triggered_id == 'pos_pnl_select_dropdown':
    # return data_viz.line_pnl(pnl_tracker_df, visible_list=['DailyPnL'])
    a_fig = data_viz.line_pnl(daily_pnl_timeseries_df[the_con], visible_list=['DailyPnL'], title='Con P&L')
    return a_fig


#@callback(Input('refresh-button', component_property='n_clicks'))
#def refresh_page_date():

#    pnl_tracker_df, pnl_tracker_update_time = get_data.get_any_data_type_df(str_date=today_str_date,
#                                                                            data_type='pnltracker',
#                                                                            acct_num=default_acct_num)
#    ret_fig = data_viz.line_pnl(pnl_tracker_df, visible_list=['DailyPnL'])



@callback(
    Output(component_id='output-container-date-picker-range', component_property='figure'), # or children
    Input(component_id='my-date-picker-range', component_property='start_date'),
    Input(component_id='my-date-picker-range', component_property='end_date'),
    Input(component_id='reset-button', component_property='n_clicks'),
    Input(component_id='year-filter-dropdown', component_property='value'),
    Input(component_id='rolling_performance_flag', component_property='value'),
)
def update_pnl_line_graph(start_date, end_date, reset_button, year_filter, rolling_perf_flag):
    triggered_id = ctx.triggered_id
    if triggered_id is None:
        return
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
    return data_viz.line_pnl(list(total_position_df.values())[1],
                             visible_list=['DYTC Model Total', 'Long Always'],
                             start_date=start_date_object,
                             end_date=end_date_object,
                             year_filter=year_filter,
                             rolling_perf_flag=rolling_perf_flag)


@callback(Output('daily-pnl-display', 'value'),
          Input('interval-component', 'n_intervals'))
def update_data(n):

    today_dt_date = datetime.now() - timedelta(days=2)
    today_str_date = today_dt_date.strftime("%Y%m%d")
    pnl_tracker_df, pnl_tracker_update_time = get_data.get_any_data_type_df(str_date=today_str_date,
                                                                            data_type='pnltracker',
                                                                            acct_num=default_acct_num)
    amount = pnl_tracker_df['DailyPnL'].iloc[-1]
    formatted_amt = f"{amount:.2f}"
    return formatted_amt


@callback(Output('intermediate-value', 'data'),
          Input('refresh-button','n_clicks'))
def clean_data(n_clicks):

    today_dt_date = datetime.now()
    today_str_date = today_dt_date.strftime("%Y%m%d")

    fills_df, fills_update_time = get_data.get_any_data_type_df(str_date=today_str_date,
                                                                dt_date=today_dt_date,
                                                                data_type='fills',
                                                                acct_num=default_acct_num)
    to_display_fills_df = fills_df[['Symbol', 'ExpirationMonth', 'Time', 'ExecId', 'Exchange', 'Side', 'NumContracts',
                                    'Price', 'AvgPrice', 'CumQty']].copy()
    open_orders_df, open_orders_update_time = get_data.get_any_data_type_df(str_date=today_str_date,
                                                                            data_type='open_orders',
                                                                            acct_num=default_acct_num)
    to_display_open_orders_df = open_orders_df[
        ['ConSym', 'OrderType', 'OrderAction', 'OrderQuantity', 'OrderStatus']].copy()
    position_pnl_df, position_pnl_update_time = get_data.get_any_data_type_df(str_date=today_str_date,
                                                                              data_type='position_pnl',
                                                                              acct_num=default_acct_num)
    position_df, position_update_time = get_data.get_any_data_type_df(str_date=today_str_date,
                                                                      acct_num=default_acct_num)
    avg_cost_df, avg_cost_update_time = get_data.get_any_data_type_df(str_date=today_str_date, data_type='avgcost',
                                                                      acct_num=default_acct_num)
    pnl_tracker_df, pnl_tracker_update_time = get_data.get_any_data_type_df(str_date=today_str_date,
                                                                            data_type='pnltracker',
                                                                            acct_num=default_acct_num)
    daily_pnl_timeseries_df, unrealized_pnl_timeseries_df, position_pnl_df = (
        get_data.create_daily_timeseries(position_df, avg_cost_df, position_pnl_df, transposed=False))
    curr_avail_pos_pnl_con_list = daily_pnl_timeseries_df.columns.get_level_values(0).unique()
    total_position_df = get_data.create_total_position_df(position_df, avg_cost_df, position_pnl_df, transposed=False)
    total_position_df = total_position_df.reset_index().rename(columns={'index': 'Contract'})
    available_position_dates = get_data.get_file_type_dates(data_type='positions', acct_num=default_acct_num)
    available_pnltracker_dates = get_data.get_file_type_dates(data_type='pnltracker', acct_num=default_acct_num)
    trade_tracker_html_doc_dict = get_data.get_trade_tracker_html_docs()
    # get the latest set of the trade tracker hmtl docs, which means get the last n elements, where n is length of cats
    tthdd_keys, tthdd_values = zip(*trade_tracker_html_doc_dict.items())
    trade_tracker_html_doc_dict = \
        dict(zip(tthdd_keys[len(trade_tracker_categories) * -1:], tthdd_values[len(trade_tracker_categories) * -1:]))
    earliest_date = available_position_dates[0]
    latest_date = available_position_dates[-1]

    the_store_data = {
        'to_display_fills_df': to_display_fills_df.to_json(orient='split', date_format='iso'),
        'to_display_open_orders_df': to_display_open_orders_df.to_json(orient='split', date_format='iso'),
        'position_pnl_df': position_pnl_df.to_json(orient='split', date_format='iso'),
        'position_df': position_df.to_json(orient='split', date_format='iso'),
        'avg_cost_df': avg_cost_df.to_json(orient='split', date_format='iso'),
        'pnl_tracker_df': pnl_tracker_df.to_json(orient='split', date_format='iso'),
        'daily_pnl_timeseries_df': daily_pnl_timeseries_df.to_json(orient='split', date_format='iso'),
        'unrealized_pnl_timeseries_df': unrealized_pnl_timeseries_df.to_json(orient='split', date_format='iso'),
        'total_position_df': total_position_df.to_json(orient='split', date_format='iso'),
        'available_position_dates': available_position_dates,
        'available_pnltracker_dates': available_pnltracker_dates,
        'trade_tracker_html_doc_dict': trade_tracker_html_doc_dict
    }

    return json.dumps(the_store_data)


@callback(
    Output('daily-pnl-display', 'color'),
    [Input('daily-pnl-display', 'value')]
)
def update_led_color(value):
    cleaned_float_value = float(re.sub(r'[^\d.-]', '', value))
    if cleaned_float_value < 0.0:
        return 'red'
    else:
        return 'green'
