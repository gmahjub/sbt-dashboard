import numpy as np
import pandas_datareader as pdr
from pathlib import Path
# import yfinance as yf
from collections import OrderedDict
import os, io, pytz, re
# import gspread
import pandas as pd
import boto3
from botocore import errorfactory
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, time, date
# from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# from google.auth.transport.requests import AuthorizedSession

# SCOPES = ['https://www.googleapis.com/auth/drive']

# S3_BUCKET_NAME = "QfsDashBucket32536C9E"
S3_BUCKET_NAME = "sbt-public-share"
s3_client = boto3.client('s3')
paginator = s3_client.get_paginator('list_objects_v2')


def generate_s3_presigned_url(object_key, expiration_seconds=604800):
    """
    Generates a pre-signed URL for an S3 object.

    Args:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key (path and filename) of the S3 object.
        expiration_seconds (int): The number of seconds the URL is valid for.

    Returns:
        str: The pre-signed URL, or None if an error occurred.
    """
    content_type = ''
    if ".html" in object_key:
        content_type = "text/html"
    try:
        url = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': object_key, 'ResponseContentType': content_type},
            ExpiresIn=expiration_seconds
        )
    except ClientError as e:
        print(f"Error generating pre-signed URL: {e}")
        return None
    return url


def create_daily_timeseries(position_df, avg_cost_df, position_pnl_df, transposed=False):

    temp_list = list(position_pnl_df.columns)
    temp_list.remove('WriteTime')
    temp_list.remove('Unnamed: 0')
    position_pnl_df.loc[position_pnl_df['Unnamed: 0'].isin(['PnlTimestamp']), temp_list] = (
        position_pnl_df.loc[position_pnl_df['Unnamed: 0'].isin(['PnlTimestamp']), temp_list].fillna(''))
    position_pnl_df.loc[position_pnl_df['Unnamed: 0'].isin(['Con_Pos', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL:',
                                                            'RealizedPnL', 'Value']), temp_list] = \
        (position_pnl_df.loc[position_pnl_df['Unnamed: 0'].isin(['Con_Pos', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL:',
                                                                 'RealizedPnL', 'Value']), temp_list].astype(float))
    position_pnl_df.loc[position_pnl_df['Unnamed: 0'].isin(['Con_Pos', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL:',
                                                            'RealizedPnL', 'Value']), temp_list] = np.where(
        position_pnl_df.loc[position_pnl_df['Unnamed: 0'].isin(['Con_Pos', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL:',
                                                                'RealizedPnL', 'Value']), temp_list] > 1e300, 0.0,
        position_pnl_df.loc[position_pnl_df['Unnamed: 0'].isin(['Con_Pos', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL:',
                                                                'RealizedPnL', 'Value']), temp_list])

    # for row_idx, row in position_pnl_df[temp_list].iterrows():
    #     try:
    #         row_sum = row.sum()
    #     except Exception as e:
    #         print("found it")

    position_pnl_df = position_pnl_df.assign(Total=position_pnl_df[temp_list].fillna(0.0).sum(axis=1))
    # temp_pp_df = position_pnl_df[temp_list].sum(axis=0)
    # temp_pp_df = temp_pp_df.assign(Total=temp_pp_df[temp_list].sum(axis=1))

    daily_pnl_ts = position_pnl_df[position_pnl_df['Unnamed: 0'].isin(['DailyPnL', 'PnlTimestamp'])].pivot(
        index='WriteTime', columns='Unnamed: 0')
    unrealized_pnl_ts = position_pnl_df[position_pnl_df['Unnamed: 0'].isin(['UnrealizedPnL', 'PnlTimestamp'])].pivot(
        index='WriteTime', columns='Unnamed: 0')
    # temp_list = list(position_pnl_df.columns)
    # temp_list.remove('Unnamed: 0')
    # temp_list.remove('WriteTime')
    # position_pnl_df[temp_list].sum(axis=1)
    return daily_pnl_ts, unrealized_pnl_ts, position_pnl_df


def create_total_position_df(position_df, avg_cost_df, position_pnl_df, transposed=True):
    """
    This function creates the total dataframe for CURRENT positions and associated position data.
    This does not contain all the time series of position, pnl, etc..
    There is a separate function to get create a time series of pnl.
    :param position_df:
    :param avg_cost_df:
    :param position_pnl_df:
    :param transposed:
    :return:
    """

    latest_ac_data = avg_cost_df.iloc[-1]
    latest_pos_data = position_df.iloc[-1]
    latest_pp_data = position_pnl_df[position_pnl_df.WriteTime == position_pnl_df.WriteTime.iloc[-1]]
    latest_pp_data = latest_pp_data.T
    latest_pp_data.columns = latest_pp_data.iloc[0]
    latest_pp_data = latest_pp_data.iloc[1:,:]
    # latest_pp_data = latest_pp_data.rename(index={'WriteTime': 'PosPnlDataWriteTime'})

    total_df = pd.concat([latest_pos_data, latest_ac_data, latest_pp_data], axis=1, ignore_index=True)
    total_df.columns = ['Contract Position', 'Position Avg Cost', 'Contract Position (Dup)', 'DailyPnL',
                        'UnrealizedPnL', 'RealizedPnL', 'MktValue', 'PnlTimestamp']
    total_df = total_df.assign(PositionTimestamp=total_df.loc['WriteTime', 'Contract Position'])
    total_df.drop('WriteTime', inplace=True)
    if transposed:
        total_df = total_df.T
        total_df.dropna(axis=1, how='all', inplace=True, subset=['Contract Position', 'Position Avg Cost'])
    else:
        total_df.dropna(axis=0, how='all', inplace=True, subset=['Contract Position', 'Position Avg Cost'])
    return total_df


def get_trade_tracker_html_docs():

    # get the list of available html file names
    pattern_match_for_pagin = f'QFS_'
    pattern_match = re.compile(r'TradeTrackerApp.*\.html$')
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=pattern_match_for_pagin)
    available_html_doc_dict = {}
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if pattern_match.search(obj['Key']):
                    # available_tta_html_list.append(obj['Key'])
                    link_to_s3_obj = generate_s3_presigned_url(obj['Key'])
                    available_html_doc_dict[obj['Key']] = link_to_s3_obj
    return dict(sorted(available_html_doc_dict.items(), key=lambda item: item[0].split('_')[-1]))


def get_file_type_dates(data_type='positions', acct_num=None):

    if acct_num is None:
        acct_num = os.getenv('DEFAULT_BROKER_ACCT_NUM')

    pattern_match = f'{acct_num}_{data_type}'
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=pattern_match)
    available_dates_list = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                available_dates_list.append(obj['Key'])
    available_dates_list = [avd.split('_')[2].split('.')[0] for avd in available_dates_list]
    # finally, sort the dates
    available_dates_list = (
        pd.Series(available_dates_list).apply(pd.to_datetime, format="%Y%m%d").
        sort_values().dt.strftime("%Y%m%d").tolist())
    return available_dates_list


def get_any_data_type_df(str_date, dt_date=None, data_type='positions_', acct_num=None):

    if acct_num is None:
        acct_num = os.getenv('DEFAULT_BROKER_ACCT_NUM')
    if data_type == 'QFS_DailySignals_':
        total_pos_fn = f'{data_type}{str_date}.csv'
    else:
        total_pos_fn = f'{acct_num}_{data_type}{str_date}.csv'

    object_s3 = None
    while object_s3 is None:
        try:
            object_s3 = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=total_pos_fn)
            break
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                # the file does not exist in the bucket
                dt_date = (dt_date - timedelta(days=1))
                str_date = dt_date.strftime("%Y%m%d")
                total_pos_fn = f'{acct_num}_{data_type}{str_date}.csv'

    object_csv = object_s3['Body'].read().decode('utf-8')
    csv_file = io.StringIO(object_csv)
    position_date_df = pd.read_csv(csv_file)
    last_modified_time_local = object_s3['LastModified'].astimezone(pytz.timezone('America/Chicago'))
    return position_date_df, last_modified_time_local.strftime("%Y%m%d %H:%m:%S")


def get_rates():
    '''
   Import Monthly US rates since 1982 from FRED St Louis
    '''
    start = '2000-01-01'
    tickers = ['GS30', 'GS10', 'GS5', 'GS3', 'GS2', 'GS1', 'GS6m', 'GS3m']
    df = pdr.get_data_fred(tickers, start)
    df.columns = ['30Y', '10Y', '5Y', '3Y', '2Y', '1Y', '6M', '3M']
    df.dropna(inplace=True)
    # Changing format from 1st day of the month to last day of the month
    df.index = df.index + pd.offsets.MonthEnd(0)
    return df


####################################
# LOAD FUNCTIONS
####################################

# load S&P 500 weights from IVV ETF stored in csv
def load_IVV_weight():

    '''
    Load weights from IVV Holdings csv => df_IVV
    link to IVV page:
    '''
    local_path = 'pages/IVV_holdings.csv'
    df_IVV = pd.read_csv(local_path, skiprows=8, header=1)
    df_IVV = df_IVV[df_IVV['Asset Class'] == 'Equity']
    df_IVV = df_IVV[['Ticker', 'Name', 'Sector', 'Asset Class', 'Weight (%)']]
    df_IVV = df_IVV.set_index('Ticker')
    df_IVV.index = df_IVV.index.str.replace('BRKB', 'BRK-B')
    df_IVV.index = df_IVV.index.str.replace('BFB', 'BF-B')
    df_IVV['Weight (%)'] = df_IVV['Weight (%)'] / 100
    return df_IVV


# # load S&P 500 weights from IVV ETF stored in csv
def load_wiki_cons(csv_path):
    '''
    Load tickers, sectors, industries etc. from wiki csv file
    => df
    '''
    df = pd.read_csv(csv_path)
    df = df.set_index('Symbol')
    return df


# ####################################
# # COMPUTE FUNCTIONS
# ####################################
#
# # Computing Daily returns
def get_returns():
    '''
    Load prices from csv and compute daily stock returns.
    output returns_df
    '''
    # google_drive url
    file_id = '1SoheVoh79lEo5HhVxR_p_XdLewRAgWdh'
    url = f'https://drive.google.com/uc?id={file_id}&export=download'
    # local file path
    local_path = 'pages/spx.csv'

    prices_csv = pd.read_csv(url).set_index('Date')
    prices_csv.index = pd.to_datetime(prices_csv.index)
    # fwd fill last prices to missing daily prices (non-trading)
    daily_prices_csv = prices_csv.asfreq('D').ffill()
    returns_df = np.log(daily_prices_csv / daily_prices_csv.shift(1))
    ## EDIT
    last_date = returns_df.index[-1]
    print(last_date)

    # last_month_end = pd.date_range(last_date, periods=1, freq='M').strftime('%Y-%m-%d')[0]
    last_month_end = last_date - pd.offsets.MonthEnd(1)
    print(last_month_end)
    returns_df = returns_df[returns_df.index <= last_month_end]
    print(returns_df.index[-1])
    return returns_df


# # Computing stock 1M, 3M, and YTD performance
def get_stock_perf(returns_df, df):
    '''
    Compute per periods from daily returns
    '''
    df_ret_summ = pd.DataFrame(np.exp((returns_df[-30:]).sum()) - 1, columns=['1M'])
    df_ret_summ['3M'] = np.exp(returns_df[-90:].sum()) - 1
    df_ret_summ['2022'] = np.exp(returns_df.loc['2022'].sum()) - 1
    df_ret_summ['YTD'] = np.exp(returns_df.loc['2023'].sum()) - 1
    df_ret_summ.index.rename('Symbol', inplace=True)
    stock_df = df.join(df_ret_summ)
    return stock_df


# # Computing sector ind returns
def get_sector_perf(returns_df, df, period='2022'):
    '''
    from df of daily returns for each stocks compute sector cum performance vs EW
    '''
    # Compute Sector / Industry daily returns - mean of stocks by sectors
    returns = returns_df.T
    returns.index.rename('Symbol', inplace=True)
    returns = df.join(returns)
    returns = returns.drop(columns='Weight')
    sector_returns = returns[returns.columns.difference(['Security', 'Sub-Industry'])].groupby('Sector').mean().T
    sector_returns.index = pd.to_datetime(sector_returns.index)
#
    ind_returns = returns[returns.columns.difference(['Security', 'Sector'])].groupby('Sub-Industry').mean().T
    ind_returns.index = pd.to_datetime(ind_returns.index)
#
    # Compute cumul sector return for line chart
    sector_cum_perf = (np.exp((sector_returns.loc['2023']).cumsum())) * 100
    sector_cum_perf.loc[pd.to_datetime('2022-12-31')] = 100
    sector_cum_perf = sector_cum_perf.sort_index()
    sector_cum_perf
#
    sector_df = pd.DataFrame(np.exp((sector_returns.loc['2023']).sum()) - 1, columns=['YTD'])
    sector_df['3M'] = np.exp(sector_returns[-90:].sum()) - 1
    sector_df['2022'] = np.exp(sector_returns.loc['2022'].sum()) - 1
#
    ind_df = pd.DataFrame(np.exp((ind_returns.loc['2023':]).sum()) - 1, columns=['YTD'])
    ind_df['3M'] = np.exp(ind_returns[-90:].sum()) - 1
    ind_df['2022'] = np.exp(ind_returns.loc['2022'].sum()) - 1
#
    return sector_df, ind_df, sector_cum_perf


# ####################################
# # FEATURE ENGINEERING
# ####################################
def join_dfs(df, df_IVV):
    df = df.join(df_IVV['Weight (%)'])
    df.sort_values(by='Weight (%)', inplace=True, ascending=False)
    df = df.rename(columns={'GICS Sector': 'Sector', 'GICS Sub-Industry': 'Sub-Industry', 'Weight (%)': 'Weight'})
    df.dropna(inplace=True)
    return df
