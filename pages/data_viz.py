####################################
# DATA VIZ - CREATE CHARTS
####################################
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date


####################################
############## RATES ###############
####################################


def line_yield_curve(df):
    '''
    Plot line chart of yield curve with animation, monthly
    '''
    df_rev = df.iloc[:, ::-1]
    tabular_df = pd.melt(df_rev.reset_index(), id_vars='DATE', value_vars=df_rev.columns, var_name='Maturity',
                         value_name='Yield')
    tabular_df['DATE'] = tabular_df['DATE'].dt.strftime('%Y-%m')

    fig = px.line(tabular_df,
                  x='Maturity',
                  y='Yield',
                  animation_frame='DATE',
                  animation_group='Maturity',
                  range_y=[0, 7],
                  markers='*',
                  text=tabular_df.Yield,
                  )
    fig.update_traces(mode='markers+text',
                      textposition='top center',
                      textfont=dict(
                          family='Arial',
                          size=14,
                      )
                      )
    fig.update_xaxes(title=None)
    fig.update_layout(title='Yield Curve Monthly Replay',
                      title_font=dict(size=20),
                      autosize=True,
                      # width=1200,
                      height=500,
                      annotations=[
                          dict(
                              text="Data Source: FRED - Federal Reserve Economic Data",
                              x=0,
                              y=-0.15,
                              xref="paper",
                              yref="paper",
                              showarrow=False
                          )
                      ]
                      )
    fig.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 100
    # fig.show(animation=dict(fromcurrent=True,mode='immediate'))
    # Auto-play animation
    # plotly.offline.plot(fig, auto_play = True)
    return fig


def surface_3d(df):
    '''
    3d surface plot - History of Yield Curve on a monthly basis from 1m to 30Y rates
    '''

    fig = go.Figure(data=[go.Surface(x=df.columns,
                                     y=df.index,
                                     z=df.values,
                                     opacity=0.95,
                                     connectgaps=True,
                                     colorscale='rdbu',
                                     showscale=True,
                                     reversescale=True,
                                     )
                          ]
                    )

    fig.update_layout(title='Yield Curve Historical Evolution',
                      title_font=dict(size=20),
                      autosize=True,
                      # width=1600,
                      height=500,
                      hovermode='closest',
                      scene={"aspectratio": {"x": 1, "y": 2.2, "z": 1},
                             'camera': {'eye': {'x': 2, 'y': 0.4, 'z': 0.8}},
                             'xaxis_title': 'Maturity',
                             'yaxis_title': 'Date',
                             'zaxis_title': 'Yield in %'
                             },
                      margin=dict(t=40),
                      annotations=[
                          dict(
                              text="Data Source: FRED - Federal Reserve Economic Data",
                              x=0,
                              y=-0.15,
                              xref="paper",
                              yref="paper",
                              showarrow=False
                          )
                      ]

                      )

    return fig


def line_spread(df):
    '''
    10-3MY spread over time
    '''
    data = df.copy()
    data['Spread'] = (df['10Y'] - df['3M']) * 100

    fig = px.area(data.reset_index(),
                  x='DATE',
                  y='Spread',
                  range_y=[-200, 400]

                  )
    fig.update_xaxes(title=None)

    fig.update_layout(title='10Y-3M Spread in bps',
                      title_font=dict(size=20),
                      autosize=True,
                      # width=1200,
                      height=500,
                      margin=dict(t=40),
                      annotations=[
                          dict(
                              text="Data Source: FRED - Federal Reserve Economic Data",
                              x=0,
                              y=-0.15,
                              xref="paper",
                              yref="paper",
                              showarrow=False
                          )
                      ]

                      )
    return fig


def heatmap2(df):
    '''
    imshow of yields per month per term in heatmap format
    '''
    data = df.T
    z = data * -1  # for colorscale to be reversed
    fig = px.imshow(z,
                    color_continuous_scale='rdbu',
                    )

    fig.update_xaxes(title=None)

    fig.update_layout(title='Yield Curve Heatmap',
                      title_font=dict(size=20),
                      autosize=True,
                      # width=1200,
                      height=500,
                      coloraxis_showscale=False,
                      annotations=[
                          dict(
                              text="Data Source: FRED - Federal Reserve Economic Data",
                              x=0,
                              y=-0.15,
                              xref="paper",
                              yref="paper",
                              showarrow=False
                          )
                      ]
                      )

    fig.update_traces(hovertemplate='Date: %{x}<br>Maturity: %{y}<br>Value: %{z}',
                      customdata=data)
    return fig


def sbt_heatmap(df_dict):
    # create df with the rate vol dataframe and the underlying price dataframe
    rate_vol_df = df_dict['Rate Vol']
    es_df = df_dict['ES']
    merged_df = rate_vol_df.merge(es_df, on=['Date'])
    merged_df['abs_rate_vol_ab'] = abs(merged_df['Rate Vol (AB)'])
    z = merged_df['Day Return'].values * 100.0
    y = merged_df['Rate Vol (AC)'].values
    x = merged_df['Rate Vol (AB)'].values
    fig = go.Figure(data=[go.Heatmap(z=z,
                                     x=x,
                                     y=y,
                                     colorscale='rdbu',
                                     showscale=True,
                                     reversescale=True,
                                     )])
    fig.update_xaxes(title=None)
    fig.update_layout(title='Rate Vol vs. ES Daily % Return, Heatmap',
                      xaxis_title='Rate Vol (AB)',
                      yaxis_title='Rate Vol (AC)',
                      title_font=dict(size=20),
                      autosize=True,
                      height=500,
                      coloraxis_showscale=False,
                      margin=dict(t=38),
                      annotations=[
                          dict(
                              text="Data Source: Prod-Dashboard Data",
                              x=0,
                              y=-0.15,
                              xref="paper",
                              yref="paper",
                              showarrow=False
                          )
                      ]
                      )
    return fig


def scatter_3d(df_dict, slider_range):
    # create df with the rate vol dataframe and the underlying price dataframe
    rate_vol_df = df_dict['Rate Vol']
    es_df = df_dict['ES']
    merged_df = rate_vol_df.merge(es_df, on=['Date'])
    merged_df['abs_rate_vol_ab'] = abs(merged_df['Rate Vol (AB)'])
    low, high = slider_range
    mask = (merged_df['Rate Vol (AC)'] > low) & (merged_df['Rate Vol (AC)'] < high)
    fig = px.scatter_3d(merged_df[mask], x='Rate Vol (AC)', y='Rate Vol (AB)', z='Day Return',
                        color='Day Return', hover_data=['Rate Vol (AC)', 'Rate Vol (AB)', 'Day Return'],
                        color_continuous_scale='rainbow')
    return fig


def heatmap(df):
    '''
    imshow of yields per month per term in heatmap format
    '''
    data = df.T
    data = data.iloc[::-1]  # to reverse order of rows in a df

    fig = go.Figure(data=[go.Heatmap(z=data.values,
                                     x=data.columns,
                                     y=data.index,

                                     colorscale='rdbu',
                                     showscale=True,
                                     reversescale=True,
                                     )])

    fig.update_xaxes(title=None)

    fig.update_layout(title='Yield Curve Heatmap',
                      title_font=dict(size=20),
                      autosize=True,
                      # width=1200,
                      height=500,
                      coloraxis_showscale=False,
                      margin=dict(t=38),
                      annotations=[
                          dict(
                              text="Data Source: FRED - Federal Reserve Economic Data",
                              x=0,
                              y=-0.15,
                              xref="paper",
                              yref="paper",
                              showarrow=False
                          )
                      ]
                      )
    return fig


####################################
############ EQUITIES ##############
####################################


def scat_ind(df, period='1M'):
    '''

    '''
    # data = df.groupby(by=['Sub-Industry','Sector',],as_index=False).mean()
    data = df[df.columns.difference(['Security'])].groupby(by=['Sub-Industry', 'Sector', ], as_index=False).mean()
    count = df.groupby(by=['Sub-Industry', 'Sector'], as_index=False).count()
    data['Count'] = count.YTD
    data = data.sort_values(by=period, ascending=False)

    fig = px.scatter(data,
                     x='Sub-Industry',
                     y=period,
                     color='Sector',
                     size='Count',
                     hover_name='Sub-Industry',
                     color_discrete_sequence=px.colors.qualitative.Plotly,
                     hover_data={period: ':.2%', 'Count': ':.0f'}
                     )
    fig.update_traces(marker=dict(
        line=dict(
            width=0.5,
            color='DarkSlateGrey')
    ))
    fig.update_layout(margin=dict(l=20, r=20),
                      title=f'Industry EW returns - {period}',
                      title_font=dict(size=20),
                      autosize=True,
                      height=800,
                      xaxis_title=None,
                      yaxis_title=None
                      )

    fig.update_yaxes(tickformat='.0%')

    return fig


def performance_tree(df):
    df['Variable'] = df.index
    color_cont = ['red', 'white', 'green']
    fig = px.treemap(df,
                     path=['Model Desc', 'Variable'],
                     values='Rate (%)',
                     color='Rate (%)',
                     color_continuous_scale=color_cont,
                     color_continuous_midpoint=0.6,
                     hover_data={'Rate (%)': ':.2%'},
                     title=''
                     )
    fig.update_layout(margin=dict(l=20, r=20, ),
                      height=600,
                      title=f'Model Accuracy Map | Model & Rate',
                      title_font=dict(size=20),
                      autosize=True,
                      annotations=[
                          dict(
                              text="Data Source: Prod-Dashboard Data",
                              x=0,
                              y=-0.05,
                              xref="paper",
                              yref="paper",
                              showarrow=False
                          )
                      ])
    return fig


def tree(df, period='1M'):
    '''

    '''

    color_cont = ['red', 'white', 'green']
    fig = px.treemap(df,
                     path=['Sector', 'Sub-Industry', 'Security'],
                     # key arg for plotly to create hierarchy based on tidy data
                     values='Weight',
                     color=period,
                     color_continuous_scale=color_cont,
                     color_continuous_midpoint=0,
                     # range_color=[-0.5,0.5],
                     hover_data={period: ':.2%', 'Weight': ':.2%'},
                     title=''
                     )

    fig.update_layout(margin=dict(l=20, r=20, ),
                      height=600,
                      title=f'S&P 500 breakdown | Sector & industry - {period}',
                      title_font=dict(size=20),
                      autosize=True,
                      annotations=[
                          dict(
                              text="Data Source: Yahoo Finance, Wikipedia, IVV ETF",
                              x=0,
                              y=-0.05,
                              xref="paper",
                              yref="paper",
                              showarrow=False
                          )
                      ]
                      )
    return fig


def bar_plot_rate_vol_binning(df_dict, display_func=None, rate_vol_name='Rate Vol (AC)'):
    # create df with the rate vol dataframe and the underlying price dataframe
    rate_vol_df = df_dict["Rate Vol"]
    # rate_vol_df = df_dict['Rate Vol (AC)']
    es_df = df_dict['ES']
    merged_df = rate_vol_df.merge(es_df, on=['Date'])
    merged_df['abs_rate_vol_ab'] = abs(merged_df['Rate Vol (AB)'])
    merged_df['Abs Day Return'] = abs(merged_df['Day Return'])
    merged_df['Market Direction'] = np.where(merged_df['Day Return'] > 0, 1, -1)
    min_val, max_val = (-57, 57)
    pos_bins = np.arange(-0.99, max_val, 1.98)[1:]
    neg_bins = np.flip(np.arange(-0.99, min_val, -1.98))
    bins = np.concatenate((neg_bins, pos_bins))
    totals_dict = {}
    for the_bin in bins:
        if np.where(bins == the_bin)[0][0] == 0:
            mask = (merged_df[rate_vol_name] < the_bin)
            # mask = (merged_df['Rate Vol (AC)'] < the_bin)
            total_cod = display_func(merged_df[mask]['CoD'])
            totals_dict[the_bin] = (total_cod, len(merged_df[mask]))
        if np.where(bins == the_bin)[0][0] == len(bins) - 1:
            mask = (merged_df[rate_vol_name] > the_bin)
            # mask = (merged_df['Rate Vol (AC)'] > the_bin)
            total_cod = display_func(merged_df[mask]['CoD'])
            num_samples = len(merged_df[mask])
            totals_dict[the_bin] = (total_cod, num_samples)
            break
        mask = (merged_df[rate_vol_name] > the_bin) & \
               (merged_df[rate_vol_name] < bins[np.where(bins == the_bin)[0][0] + 1])
        # mask = (merged_df['Rate Vol (AC)'] > the_bin) & \
        #       (merged_df['Rate Vol (AC)'] < bins[np.where(bins == the_bin)[0][0] + 1])
        total_cod = display_func(merged_df[mask]['CoD'])
        totals_dict[the_bin] = (total_cod, len(merged_df[mask]))
    a = list(totals_dict.values())
    total_cod_list, num_samples_list = map(list, zip(*a))
    if display_func is np.sum:
        aggregation_value = "Total CoD"
        title = 'Total ES Points Delta vs. ' + rate_vol_name
        # title = 'Total ES Points Delta vs. Rate Vol (AC)'
    elif display_func is np.std:
        aggregation_value = "Daily Volatility"
        title = 'ES Points Daily Volatility vs. ' + rate_vol_name
        # title = 'ES Points Daily Volatility vs. Rate Vol (AC)'
    elif display_func is np.mean:
        aggregation_value = "Mean CoD"
        title = 'ES Points, Mean Return vs. ' + rate_vol_name
    totals_dict = {"bins": list(totals_dict.keys()), aggregation_value: total_cod_list,
                   "Number of Samples": num_samples_list}
    rate_vol_bin_df = pd.DataFrame.from_dict(totals_dict)
    fig = px.bar(rate_vol_bin_df,
                 x='bins',
                 y=aggregation_value,
                 hover_data=['Number of Samples', aggregation_value],
                 color='Number of Samples',
                 labels={aggregation_value: aggregation_value + ', ES Pts', 'bins': rate_vol_name},
                 text=aggregation_value, color_continuous_scale='rainbow',
                 title=title)
    return fig


def create_generic_bar_plot(df, title):

    fig = px.bar(df,
                 x='Contract',
                 y='Max Trade Size',
                 hover_data=['Max Trade Size', 'Max Slip (% or Ticks)', 'Con Exp', 'Trade Date'],
                 color='Max Trade Size',
                 # labels={aggregation_value: aggregation_value + ', ES Pts', 'bins': rate_vol_name},
                 text='Max Trade Size', color_continuous_scale='rainbow',
                 title=title)
    return fig




def bar_plot_accuracy_stats(bar_plot_df):
    fig = px.bar(bar_plot_df,
                 x=bar_plot_df.index,
                 y=['Rate (%)'],
                 color_discrete_sequence=['lightblue'],
                 barmode='group',
                 )
    fig.add_hline(y=0.60,
                  line_width=3,
                  line_dash='dash',
                  line_color='green')
    fig.update_layout(margin=dict(l=20, r=20),
                      title=f'Accuracy Rates',
                      title_font=dict(size=20),
                      autosize=True,
                      height=600,
                      xaxis_title=None,
                      yaxis_title=None
                      )
    fig.update_yaxes(tickformat='.2%')
    return fig


def bar_sec(df):
    '''

    '''
    df = df.groupby(by='Sector').mean()
    df = df.sort_values(by='YTD', ascending=False)

    fig = px.bar(df,
                 x=df.index,
                 y=['YTD', '3M', '2022'],
                 color_discrete_sequence=['indianred', 'grey', 'darkgrey'],
                 barmode='group',
                 )

    fig.update_layout(margin=dict(l=20, r=20),
                      title=f'Sector EW returns',
                      title_font=dict(size=20),
                      autosize=True,
                      height=600,
                      xaxis_title=None,
                      yaxis_title=None
                      )

    fig.update_yaxes(tickformat='.2%')

    return fig


def scat_rate_vol(df_dict, x_var, y_var='Day Return', color_var='SPX Vol (BQ)', size_var='abs_rate_vol_ab', title=None):
    '''

    '''

    # create df with the rate vol dataframe and the underlying price dataframe
    rate_vol_df = df_dict['Rate Vol']
    es_df = df_dict['ES']
    merged_df = rate_vol_df.merge(es_df, on=['Date'])
    merged_df['abs_rate_vol_ab'] = abs(merged_df['Rate Vol (AB)'])
    merged_df['Abs Day Return'] = abs(merged_df['Day Return'])
    merged_df['Market Direction'] = np.where(merged_df['Day Return'] > 0, 1, -1)
    # color should be Market Direction and size should be Abs Day Return
    if title is None:
        title = x_var + f' vs. Daily ES Return'
    fig = px.scatter(merged_df,
                     x=x_var,
                     y=y_var,
                     color=color_var,
                     size=size_var,
                     # hover_name='Security',
                     size_max=40,
                     color_discrete_sequence=px.colors.qualitative.Plotly,
                     color_continuous_scale='rainbow',
                     hover_data={'Rate Vol (AC)': ':.2',
                                 'Rate Vol (AB)': ':.2',
                                 'Day Return': ':2%'},
                     title=title

                     )
    fig.update_traces(marker=dict(
        line=dict(
            width=0.5,
            color='DarkSlateGrey')
    ))
    fig.update_layout(margin=dict(l=20, r=20),
                      height=600,
                      title_font=dict(size=20),
                      autosize=True,
                      )
    if y_var == 'Day Return':
        fig.update_yaxes(tickformat='.2%')
    else:
        fig.update_yaxes(tickformat='.3')
    fig.update_xaxes(tickformat='.3')

    return fig


def scat_stock(df):
    '''

    '''

    fig = px.scatter(df,
                     x='2022',
                     y='YTD',
                     color='Sector',
                     size='Weight',
                     hover_name='Security',
                     size_max=40,
                     color_discrete_sequence=px.colors.qualitative.Plotly,
                     hover_data={'2022': ':.2%',
                                 'YTD': ':.2%',
                                 'Weight': ':2%'},
                     title=f'Stock returns - YTD vs 2022'

                     )
    fig.update_traces(marker=dict(
        line=dict(
            width=0.5,
            color='DarkSlateGrey')
    ))
    fig.update_layout(margin=dict(l=20, r=20),
                      height=600,
                      title_font=dict(size=20),
                      autosize=True,
                      )

    fig.update_yaxes(tickformat='.0%')
    fig.update_xaxes(tickformat='.0%')

    return fig


def rate_volatility_line_chart(df_dict):
    df = df_dict['Rate Vol']
    df.set_index('Date', inplace=True)
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Scatter(x=df.index, y=df['Rate Vol (AC)'], mode="lines", name='Rate Vol (AC)'), secondary_y=True)
    fig.add_trace(go.Scatter(x=df.index, y=df['Actual Return'] * 100.0, mode="lines", name='ES % Return'),
                  secondary_y=False)
    fig.add_trace(go.Scatter(x=df.index, y=df['VIX Return'] * 100.0, mode="lines", name='VIX % Return'),
                  secondary_y=True)
    # fig.add_trace(go.Scatter(x=df.index, y=df['Rate Vol (AB)'], name='Rate Vol (AB)'), secondary_y=True)
    # fig.add_trace(go.Scatter(x=df.index, y=df['SmoothAC-SPX Correlation'], name='SmoothAC-SPX % Correl'),
    #              secondary_y=True)

    # fig.add_trace(go.Bar(x=df.index, y=df['Actual Return']*100.0, name='ES % Return'), secondary_y=False)
    # fig.add_trace(go.Bar(x=df.index, y=df['VIX Return']*100.0, name='VIX % Return'), secondary_y=True)
    fig.add_trace(go.Bar(x=df.index, y=df['Rate Vol (AB)'], name='Rate Vol (AB)'), secondary_y=True)
    fig.add_trace(go.Bar(x=df.index, y=df['SmoothAC-SPX Correlation'], name='SmoothAC-SPX % Correl'),
                  secondary_y=True)

    # fig= px.line(df,
    #              x=df.index,
    #              y=['Rate Vol (AC)', 'Actual Return', 'VIX Return'],
    #              color_discrete_sequence=px.colors.qualitative.Plotly,
    #              title=f'Rate Volatility Performance | VIX Comparison')
    # fig.add_bar(x=df.index, y=df['Rate Vol (AB)'].values, name='Rate Vol (AB)', secondary_y=True)
    # fig.add_bar(x=df.index, y =df['SmoothAC-SPX Correlation'].values, name='SmoothAC-SPX Correl', secondary_y=True)

    fig.update_layout(margin=dict(l=20, r=20),
                      height=600,
                      title_font=dict(size=20),
                      autosize=True,
                      xaxis_title=None,
                      yaxis_title=None,
                      title=dict(text=f'Rate Volatility Performance | VIX Comparison')
                      )
    fig.update_yaxes(title_text="<b>ES</b> % Return", tickformat='.2f', secondary_y=False)
    fig.update_yaxes(title_text="<b>Remaining Vars</b> % Values", secondary_y=True)
    fig.update_xaxes(title_text='Date',
                     rangebreaks=[
                         dict(bounds=["sat", "mon"]),  # hide weekends
                     ]
                     )
    return fig


def pnl_histogram(df_ms, model_list_df, model_name):
    df = df_ms['Histogram Data']
    x_var_name = model_list_df[model_list_df.Name == model_name]['Realtime Column'].iloc[0]
    fig = px.histogram(df, x=x_var_name, color_discrete_sequence=['blue'])
    return fig


def line_pnl(df, visible_list, start_date=None, end_date=None,
             year_filter=None, rolling_perf_flag=True, title='Daily Pnl Tracker'):
    '''
    Plot models' pnl against being long always
    '''

    # max_visualization_data = 90
    # zeroed = False
    if df.index.name == 'WriteTime':
        df.reset_index(inplace=True)
    df.WriteTime = pd.to_datetime(df.WriteTime)
    data = df[['WriteTime'] + visible_list]
    filtered_data = data
    if start_date is None and end_date is None:
        filtered_data = data.copy(deep=True)
    filtered_data.set_index('WriteTime', inplace=True)
    filtered_data = filtered_data.iloc[-550:,:]
    fig = px.line(filtered_data,
                  y=filtered_data.columns,
                  x=filtered_data.index,
                  color_discrete_sequence=px.colors.qualitative.Plotly,
                  title=f'{title}'
                  ).update_traces(visible='legendonly', selector=lambda t: not t.name in visible_list)
    fig.add_hline(y=0.0,
                  line_width=3,
                  line_dash='dash',
                  line_color='green')
    fig.update_layout(margin=dict(l=20, r=20),
                      height=600,
                      title_font=dict(size=20),
                      autosize=True,
                      xaxis_title=None,
                      yaxis_title=None,
                      )
    fig.update_yaxes(tickformat='.2f')
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),  # hide weekends
        ]
    )
    return fig


def line_sector(sector_cum_perf_df):
    '''
    Plot cumulative performances of Sectors(EW) vs EW of Sectors
    '''
    data = sector_cum_perf_df.resample('B').mean()

    fig = px.line(data,
                  y=data.columns,
                  x=data.index,
                  color_discrete_sequence=px.colors.qualitative.Plotly,
                  title=f'Cumulative growth | Sector EW - YTD'

                  )

    fig.update_layout(margin=dict(l=20, r=20),
                      height=600,
                      title_font=dict(size=20),
                      autosize=True,
                      xaxis_title=None,
                      yaxis_title=None,
                      )

    fig.update_yaxes(tickformat='.2f')
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),  # hide weekends
        ]
    )

    return fig
