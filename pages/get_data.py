import numpy as np
import pandas_datareader as pdr
from pathlib import Path
# import yfinance as yf
from collections import OrderedDict
import os, io, pytz, re
# import gspread
import pandas as pd
import boto3
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


def create_daily_timeseries(position_df, avg_cost_df, position_pnl_df, transposed=False):

    temp_list = list(position_pnl_df.columns)
    temp_list.remove('WriteTime')
    temp_list.remove('Unnamed: 0')
    position_pnl_df.loc[position_pnl_df['Unnamed: 0'].isin(['Con_Pos', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL:',
                                                            'RealizedPnL', 'Value']), temp_list] = \
        (position_pnl_df.loc[position_pnl_df['Unnamed: 0'].isin(['Con_Pos', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL:',
                                                                 'Value']), temp_list].astype(float))
    position_pnl_df.loc[position_pnl_df['Unnamed: 0'].isin(['Con_Pos', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL:',
                                                            'RealizedPnL', 'Value']), temp_list] = np.where(
        position_pnl_df.loc[position_pnl_df['Unnamed: 0'].isin(['Con_Pos', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL:',
                                                                'RealizedPnL', 'Value']), temp_list] > 1e300, 0.0,
        position_pnl_df.loc[position_pnl_df['Unnamed: 0'].isin(['Con_Pos', 'DailyPnL', 'UnrealizedPnL', 'RealizedPnL:',
                                                                'RealizedPnL', 'Value']), temp_list])
    position_pnl_df = position_pnl_df.assign(Total=position_pnl_df[temp_list].sum(axis=1))
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

    pattern_match_for_pagin = f'QFS_'
    pattern_match = re.compile(r'TradeTrackerApp.*\.html$')
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=pattern_match_for_pagin)
    available_tta_html_list = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if pattern_match.search(obj['Key']):
                    available_tta_html_list.append(obj['Key'])
    return available_tta_html_list


def get_file_type_dates(data_type='positions', acct_num=None):

    if acct_num is None:
        acct_num = os.getenv('DEFAULT_BROKER_ACCT_NUM')

    pattern_match = f'{acct_num}_{data_type}_'
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


def get_any_data_type_df(str_date, data_type='positions', acct_num=None):

    if acct_num is None:
        acct_num = os.getenv('DEFAULT_BROKER_ACCT_NUM')
    total_pos_fn = f'{acct_num}_{data_type}_{str_date}.csv'

    object_s3 = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=total_pos_fn)
    object_csv = object_s3['Body'].read().decode('utf-8')
    csv_file = io.StringIO(object_csv)
    position_date_df = pd.read_csv(csv_file)
    last_modified_time_local = object_s3['LastModified'].astimezone(pytz.timezone('America/Chicago'))
    return position_date_df, last_modified_time_local.strftime("%Y%m%d %H:%m:%S")

# def get_creds():
#
#     # credentials location
#
#     home_dir = Path.home()
#     credentials_location = "/Users/ghazymahjub/workspace/google_api_credentials.json"
#
#     creds = None
#     # The file token.json stores the user's access and refresh tokens, and is
#     # created automatically when the authorization flow completes for the first
#     # time.
#     if os.path.exists('token.json'):
#         creds = Credentials.from_authorized_user_file('token.json', SCOPES)
#     # If there are no (valid) credentials available, let the user log in.
#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             creds.refresh(Request())
#         else:
#             try:
#                 flow = InstalledAppFlow.from_client_secrets_file(
#                     credentials_location, SCOPES)
#             except FileNotFoundError as fe:
#                 credentials_location = str(home_dir) + '/workspace/google_api_credentials.json'
#                 flow = InstalledAppFlow.from_client_secrets_file(
#                     credentials_location, SCOPES)
#             creds = flow.run_local_server(port=0)
#         # Save the credentials for the next run
#         with open('token.json', 'w') as token:
#             token.write(creds.to_json())
#     return creds


# def translate_column_to_number(col_letters):
#     from string import ascii_uppercase
#     letters = {letter: index for index, letter in enumerate(ascii_uppercase, start=1)}
#     if len(col_letters) == 1:
#         return letters[col_letters]
#     elif len(col_letters) == 2:
#         return letters[col_letters[0]] * 26 + letters[col_letters[1]]
#     # we will stop at two letters for column name as we won't let the DYTC spreadsheet get longer than that.
#     else:
#         return -1


# def get_active_scenarios(scenario_search_date=None):
#
#     fn = 'Daily Treasury Yield Curve - Plot'
#     creds = get_creds()
#     client = gspread.authorize(creds)
#     sheet_name = 'Treasury Yield Curve'
#     worksheet = client.open(fn).worksheet(sheet_name)
#     str_ssd = worksheet.col_values(1)[-1]
#     cell = worksheet.find(str_ssd)
#     # get all values from the cell.row - these are the values that we compare to the DYTC Scenarios dataframe
#     values_list = worksheet.row_values(cell.row)
#     lag_values_list = worksheet.row_values(cell.row-1)
#
#     # next, get the DYTC Scenarios file into a dataframe
#     scenarios_nm = 'DYTC Scenarios'
#     scen_ws = client.open(scenarios_nm).worksheet(scenarios_nm)
#     scenarios_df = pd.DataFrame(scen_ws.get_all_records())
#
#     return_val = scenarios_df.apply(get_active_scenarios_helper, dytc_values=[values_list, lag_values_list], axis=1)
#     filtered_return_val = return_val[return_val!=-1]
#     consolidated_dict = {}
#     for active_scenario in filtered_return_val:
#         for k, v in active_scenario.items():
#             if k in consolidated_dict:
#                 consolidated_dict[k].append(v)
#             else:
#                 consolidated_dict[k] = [v]
#     return_df = pd.DataFrame().from_dict(consolidated_dict)
#     update_time = datetime.now()
#     #populate_prod_dashboard_scenario_history(return_df, creds, update_time.date())
#     return {'Activated Scenarios': return_df}, update_time


# def populate_prod_dashboard_scenario_history(data_df, creds, update_date):
#
#     formatted_update_date = update_date.strftime("%m/%d/%y")
#     gc = gspread.Client(auth=creds)
#     gc.session = AuthorizedSession(creds)
#     google_drive_filename = 'Prod-Dashboard Data'
#     sheet = gc.open(google_drive_filename)
#     worksheet = sheet.worksheet('Active Scenario History')
#     date_values = worksheet.col_values(1)
#     last_date = date_values[-1]
#     last_row = len(date_values)
#     insert = False
#     if last_date == 'Date':
#         insert = True
#     elif last_date < formatted_update_date:
#         insert = True
#     if insert:
#         worksheet.update_cell(last_row+1,1,formatted_update_date)
#         worksheet.update_cell(last_row+1,2,str(data_df.Leverage.sum()))
#         worksheet.update_cell(last_row+1,3,str(len(data_df)))
#         worksheet.update_cell(last_row+1,4,str(data_df['Scenario Id'].values))
#         worksheet.update_cell(last_row+1,5,str(data_df[data_df['Scenario Bias'] == 'L']['Scenario Id'].values))
#         worksheet.update_cell(last_row+1,6,
#                               str(data_df[(data_df['Scenario Bias'] == 'L') & (data_df['Leverage'] == 1)] \
#                                                    ['Scenario Id'].values))
#         worksheet.update_cell(last_row + 1, 7,
#                               str(data_df[(data_df['Scenario Bias'] == 'L') & (data_df['Leverage'] == 2)] \
#                                       ['Scenario Id'].values))
#         worksheet.update_cell(last_row + 1, 8, str(data_df[data_df['Scenario Bias'] == 'S']['Scenario Id'].values))
#         worksheet.update_cell(last_row + 1, 9,
#                               str(data_df[(data_df['Scenario Bias'] == 'S') & (data_df['Leverage'] == 1)] \
#                                       ['Scenario Id'].values))
#         worksheet.update_cell(last_row + 1, 10,
#                               str(data_df[(data_df['Scenario Bias'] == 'S') & (data_df['Leverage'] == 2)] \
#                                       ['Scenario Id'].values))


# def get_active_scenarios_helper(row, dytc_values):
#
#     return_dict = {}
#     eval_res_list = []
#     for index, values in row.items():
#         col_num = translate_column_to_number(index)
#         if index == 'Scenario Id' or values == '':
#             continue
#         if index == 'Scenario Bias':
#             final_res = all(eval_res_list)
#             if final_res:
#                 return_dict['Active?'] = final_res
#                 return_dict[index] = values
#                 return_dict['Leverage'] = row['Leverage']
#                 return_dict['Scenario Description'] = row['Scenario Description']
#                 return_dict['Notes'] = row['Notes']
#                 return_dict['Scenario Id'] = row['Scenario Id']
#                 return return_dict
#             else:
#                 return -1
#         the_value = dytc_values[0][col_num-1]
#         the_lagged_value = dytc_values[1][col_num-1]
#         conditions_list = str(values).split(',')
#         for cl in conditions_list:
#             eval_or_list = []
#             for cl_or in cl.split('or'):
#                 if 'lag(1)' in cl_or:
#                     eval_res = eval(str(the_lagged_value) + cl_or.split('lag(1)')[1])
#                     eval_or_list.append(eval_res)
#                 elif the_value == '' and cl_or != '':
#                     eval_or_list.append(False)
#                 elif any(ele in cl_or for ele in ['>', '<', '=<', '=>', '==', '!=']):
#                     eval_res = eval(str(the_value) + cl_or)
#                     eval_or_list.append(eval_res)
#                 else:
#                     # this is where we just have a number
#                     eval_res = eval(str(the_value) + '==' + cl_or)
#                     eval_or_list.append(eval_res)
#             eval_res_list.append(any(eval_or_list))


# def get_trade_plans(name_contains=None):
#
#     creds = get_creds()
#     try:
#         service = build('drive', 'v3', credentials=creds)
#         files = []
#         page_token = None
#         while True:
#             if name_contains is None:
#                 response = service.files().list(q="name contains 'Trade Plan' and (name contains "
#                                                   "'SB Trades' or name contains 'SB Trader')",
#                                                 spaces='drive',
#                                                 fields='nextPageToken, '
#                                                 'files(id, name, webViewLink, modifiedTime, createdTime, '
#                                                        'sharedWithMeTime)',
#                                                 pageToken=page_token).execute()
#             else:
#                 response = service.files().list(q=name_contains,
#                                                 spaces='drive',
#                                                 fields='nextPageToken, '
#                                                 'files(id, name, webViewLink, modifiedTime, createdTime, '
#                                                        'sharedWithMeTime)',
#                                                 pageToken=page_token).execute()
#             for file in response.get('files', []):
#                 print(F'Found file: {file.get("name")}, {file.get("id")}, {file.get("webViewLink")}, '
#                       F'{file.get("modifiedTime")}, {file.get("createdTime")}, {file.get("sharedWithMeTime")}')
#             files.extend(response.get('files', []))
#             page_token = response.get('nextPageToken', None)
#             if page_token is None:
#                 break
#     except HttpError as error:
#         print(f'An error occurred: {error}')
#         files = None
#     # exclude the trade plan of the most recent day, this is the paid trade plan from the Patreon
#     dict_for_sort = {}
#     for f in files:
#         # sort
#         if "Trade Plan" in f['name'] or 'Final Signal' in f['name'] or "Prelim Signal" in f['name']:
#             if datetime.strptime(f['name'].split('-')[1].strip(), "%m/%d/%Y").date() != datetime.now().date():
#                 # files.remove(f)
#                 dict_for_sort[f['name']] = f
#         else:
#             dict_for_sort[f['name']] = f
#     ordered_files_dict = OrderedDict(sorted(dict_for_sort.items(), reverse=True))
#     return list(ordered_files_dict.values())


# def get_dytc_px_levels(df_ms, neg_sigmas=0.5, pos_sigmas=0.5):
#
#     es_px_df = df_ms['ES']
#     dytc_px_lvls_df = df_ms['DYTC Px Levels'].copy(deep=True)
#     work_df = es_px_df[['Date', 'Settle', 'Open', 'High', 'Low']].merge(dytc_px_lvls_df, on='Date', how='outer')
#     work_df['Dist Std'] = pd.to_numeric(work_df['Dist Std']).fillna(0.0)
#     work_df[['Settle', 'Open', 'High', 'Low']] = work_df[['Settle', 'Open', 'High', 'Low']].apply(pd.to_numeric,
#                                                                                                   errors='coerce')
#     work_df['OB Level'] = np.where(work_df['Dist Std'] == 0.0, 0.0, (1+(work_df['Expected Return']+
#                                                                      pos_sigmas*work_df['Dist Std']/100.0)))
#     work_df['OB Level'] = work_df['OB Level']*work_df['Settle'].shift(1)
#     work_df['OS Level'] = np.where(work_df['Dist Std'] == 0.0, 0.0, (1+(work_df['Expected Return']-
#                                                                      neg_sigmas*work_df['Dist Std']/100.0)))
#     work_df['OS Level'] = work_df['OS Level']*work_df['Settle'].shift(1)
#     conditions_ob = [(work_df['OB Level'] == 0.0),
#                      (work_df['OB Level'] < work_df['High']) & (work_df['OB Level'] < work_df['Open']),
#                      (work_df['OB Level'] < work_df['High'])]
#     values_ob = [0.0, work_df['Open'] - work_df['Settle'],
#               work_df['OB Level'] - work_df['Settle']]
#     work_df['Pts Ret, OBL'] = np.select(conditions_ob, values_ob)
#     conditions_os = [(work_df['OS Level'] == 0),
#                      (work_df['OS Level'] > work_df['Low']) & (work_df['OS Level'] > work_df['Open']),
#                      work_df['OS Level'] > work_df['Low']]
#     values_os = [0.0, work_df['Settle'] - work_df['Open'],
#               work_df['Settle'] - work_df['OS Level']]
#     flag_vals = [0, 1, 2]
#     work_df['Pts Ret, OSL'] = np.select(conditions_os, values_os)
#     work_df['Flag, OBL'] = np.select(conditions_ob, flag_vals)
#     work_df['Flag, OSL'] = np.select(conditions_os, flag_vals)
#
#     flat_obl_sum = work_df[(work_df['DYTC Units'] == 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].sum()
#     flat_obl_cnt = work_df[(work_df['DYTC Units'] == 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].count()
#     flat_obl_mean = work_df[(work_df['DYTC Units'] == 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].mean()
#     flat_obl_std = work_df[(work_df['DYTC Units'] == 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].std()
#
#     long_obl_sum = work_df[(work_df['DYTC Units'] > 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].sum()
#     long_obl_cnt = work_df[(work_df['DYTC Units'] > 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].count()
#     long_obl_mean = work_df[(work_df['DYTC Units'] > 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].mean()
#     long_obl_std = work_df[(work_df['DYTC Units'] > 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].std()
#
#     short_obl_sum = work_df[(work_df['DYTC Units'] < 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].sum()
#     short_obl_cnt = work_df[(work_df['DYTC Units'] < 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].count()
#     short_obl_mean = work_df[(work_df['DYTC Units'] < 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].mean()
#     short_obl_std = work_df[(work_df['DYTC Units'] < 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].std()
#
#     flat_obs_sum = work_df[(work_df['DYTC Units'] == 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].sum()
#     flat_obs_cnt = work_df[(work_df['DYTC Units'] == 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].count()
#     flat_obs_mean = work_df[(work_df['DYTC Units'] == 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].mean()
#     flat_obs_std = work_df[(work_df['DYTC Units'] == 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].std()
#
#     long_obs_sum = work_df[(work_df['DYTC Units'] > 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].sum()
#     long_obs_cnt = work_df[(work_df['DYTC Units'] > 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].count()
#     long_obs_mean = work_df[(work_df['DYTC Units'] > 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].mean()
#     long_obs_std = work_df[(work_df['DYTC Units'] > 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].std()
#
#     short_obs_sum = work_df[(work_df['DYTC Units'] < 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].sum()
#     short_obs_cnt = work_df[(work_df['DYTC Units'] < 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].count()
#     short_obs_mean = work_df[(work_df['DYTC Units'] < 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].mean()
#     short_obs_std = work_df[(work_df['DYTC Units'] < 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].std()
#
#     # osl by flag
#     zero_flag_osl_sum = work_df[(work_df['Flag, OSL'] == 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].sum()
#     zero_flag_osl_cnt =
#     work_df[(work_df['Flag, OSL'] == 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].count()
#     zero_flag_osl_mean =
#     work_df[(work_df['Flag, OSL'] == 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].mean()
#     zero_flag_osl_std = work_df[(work_df['Flag, OSL'] == 0) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].std()
#
#     one_flag_osl_sum = work_df[(work_df['Flag, OSL'] == 1) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].sum()
#     one_flag_osl_cnt = work_df[(work_df['Flag, OSL'] == 1) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].count()
#     one_flag_osl_mean = work_df[(work_df['Flag, OSL'] == 1) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].mean()
#     one_flag_osl_std = work_df[(work_df['Flag, OSL'] == 1) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].std()
#
#     two_flag_osl_sum = work_df[(work_df['Flag, OSL'] == 2) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].sum()
#     two_flag_osl_cnt = work_df[(work_df['Flag, OSL'] == 2) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].count()
#     two_flag_osl_mean = work_df[(work_df['Flag, OSL'] == 2) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].mean()
#     two_flag_osl_std = work_df[(work_df['Flag, OSL'] == 2) & (work_df['Pts Ret, OSL'] != 0.0)]['Pts Ret, OSL'].std()
#
#     # obl by flag
#     zero_flag_obl_sum = work_df[(work_df['Flag, OBL'] == 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].sum()
#     zero_flag_obl_cnt =
#     work_df[(work_df['Flag, OBL'] == 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].count()
#     zero_flag_obl_mean =
#     work_df[(work_df['Flag, OBL'] == 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].mean()
#     zero_flag_obl_std = work_df[(work_df['Flag, OBL'] == 0) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].std()
#
#     one_flag_obl_sum = work_df[(work_df['Flag, OBL'] == 1) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].sum()
#     one_flag_obl_cnt = work_df[(work_df['Flag, OBL'] == 1) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].count()
#     one_flag_obl_mean = work_df[(work_df['Flag, OBL'] == 1) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].mean()
#     one_flag_obl_std = work_df[(work_df['Flag, OBL'] == 1) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].std()
#
#     two_flag_obl_sum = work_df[(work_df['Flag, OBL'] == 2) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].sum()
#     two_flag_obl_cnt = work_df[(work_df['Flag, OBL'] == 2) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].count()
#     two_flag_obl_mean = work_df[(work_df['Flag, OBL'] == 2) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].mean()
#     two_flag_obl_std = work_df[(work_df['Flag, OBL'] == 2) & (work_df['Pts Ret, OBL'] != 0.0)]['Pts Ret, OBL'].std()
#
#     print("flat obl")
#     print(flat_obl_sum, flat_obl_mean, flat_obl_std, flat_obl_cnt)
#     print("long obl")
#     print(long_obl_sum, long_obl_mean, long_obl_std, long_obl_cnt)
#     print("short obl")
#     print(short_obl_sum, short_obl_mean, short_obl_std, short_obl_cnt)
#
#     print("flat obs")
#     print(flat_obs_sum, flat_obs_mean, flat_obs_std, flat_obs_cnt)
#     print("long obs")
#     print(long_obs_sum, long_obs_mean, long_obs_std, long_obs_cnt)
#     print("short obs")
#     print(short_obs_sum, short_obs_mean, short_obs_std, short_obs_cnt)
#
#     print ("obl zero flag")
#     print(zero_flag_obl_sum, zero_flag_obl_mean, zero_flag_obl_std, zero_flag_obl_cnt)
#     print("obl one flag")
#     print(one_flag_obl_sum, one_flag_obl_mean, one_flag_obl_std, one_flag_obl_cnt)
#     print("obl two flag")
#     print(two_flag_obl_sum, two_flag_obl_mean, two_flag_obl_std, two_flag_obl_cnt)
#
#     print("osl zero flag")
#     print(zero_flag_osl_sum, zero_flag_osl_mean, zero_flag_osl_std, zero_flag_osl_cnt)
#     print("osl one flag")
#     print(one_flag_osl_sum, one_flag_osl_mean, one_flag_osl_std, one_flag_osl_cnt)
#     print("osl two flag")
#     print(two_flag_osl_sum, two_flag_osl_mean, two_flag_osl_std, two_flag_osl_cnt)
#
#     return 1
#
#
# def create_re_analysis_df(df_ms):
#
#     rea_df = df_ms['Risk Event Analysis'].copy(deep=True)
#     rea_df.sort_values('Date', inplace=True)
#
#     es_cod_tm1d_tot = rea_df[rea_df['Risk Event Time'] == 't-1d']['ES CoD'].sum()
#     es_cod_t_tot = rea_df[rea_df['Risk Event Time'] == 't']['ES CoD'].sum()
#     es_cod_tp1d_tot = rea_df[rea_df['Risk Event Time'] == 't+1d']['ES CoD'].sum()
#
#     num_samples_tm1d = len(rea_df[rea_df['Risk Event Time'] == 't-1d'])
#     num_samples_t = len(rea_df[rea_df['Risk Event Time'] == 't'])
#     num_samples_tp1d = len(rea_df[rea_df['Risk Event Time'] == 't+1d'])
#
#     es_cod_tm1d_mean = rea_df[rea_df['Risk Event Time'] == 't-1d']['ES CoD'].mean()
#     if num_samples_tm1d > 1:
#         es_cod_tm1d_std = rea_df[rea_df['Risk Event Time'] == 't-1d']['ES CoD'].std()
#     else:
#         es_cod_tm1d_std = 0.0
#     es_cod_tm1d_esr = es_cod_tm1d_mean/es_cod_tm1d_std*np.sqrt(num_samples_tm1d)
#     es_cod_tm1d_min = rea_df[rea_df['Risk Event Time'] == 't-1d']['ES CoD'].min()
#     es_cod_tm1d_max = rea_df[rea_df['Risk Event Time'] == 't-1d']['ES CoD'].max()
#     es_cod_tm1d_median = rea_df[rea_df['Risk Event Time'] == 't-1d']['ES CoD'].median()
#     es_cod_tm1d_posmean = np.mean(list(filter(lambda x: x>0,rea_df[rea_df['Risk Event Time'] == 't-1d']['ES CoD'].
#                                               values)))
#     es_cod_tm1d_negmean = np.mean(list(filter(lambda x: x<0,rea_df[rea_df['Risk Event Time'] == 't-1d']['ES CoD'].
#                                               values)))
#
#     dytc_cod_tm1d_tot = rea_df[rea_df['Risk Event Time'] == 't-1d']['DYTC Model CoD'].sum()
#     dytc_cod_t_tot = rea_df[rea_df['Risk Event Time'] == 't']['DYTC Model CoD'].sum()
#     dytc_cod_tp1d_tot = rea_df[rea_df['Risk Event Time'] == 't+1d']['DYTC Model CoD'].sum()
#
#     dytc_num_samples_tm1d = (rea_df[rea_df['Risk Event Time'] == 't-1d']['DYTC Model CoD'] != 0.0).sum()
#     dytc_num_samples_t = (rea_df[rea_df['Risk Event Time'] == 't']['DYTC Model CoD'] != 0.0).sum()
#     dytc_num_samples_tp1d = (rea_df[rea_df['Risk Event Time'] == 't+1d']['DYTC Model CoD'] != 0.0).sum()
#
#     data_dict = {'t-1d': [es_cod_tm1d_tot, es_cod_tm1d_mean, es_cod_tm1d_std, es_cod_tm1d_esr, es_cod_tm1d_min,
#                           es_cod_tm1d_max, es_cod_tm1d_median, es_cod_tm1d_posmean, es_cod_tm1d_negmean,
#                           num_samples_tm1d, dytc_cod_tm1d_tot, dytc_num_samples_tm1d],
#                  't': [es_cod_t_tot, num_samples_t, dytc_cod_t_tot, dytc_num_samples_t],
#                  't+1d': [es_cod_tp1d_tot, num_samples_tp1d, dytc_cod_tp1d_tot, dytc_num_samples_tp1d]}
#     for the_time in ['t', 't+1d']:
#         es_cod_mean = rea_df[rea_df['Risk Event Time'] == the_time]['ES CoD'].mean()
#         data_dict[the_time].insert(1, es_cod_mean)
#         es_cod_std = rea_df[rea_df['Risk Event Time'] == the_time]['ES CoD'].std()
#         data_dict[the_time].insert(2, es_cod_std)
#         if the_time == 't':
#             es_cod_esr = es_cod_mean / es_cod_std * np.sqrt(num_samples_t)
#         elif the_time == 't+1d':
#             es_cod_esr = es_cod_mean / es_cod_std * np.sqrt(num_samples_tp1d)
#         data_dict[the_time].insert(3, es_cod_esr)
#         es_cod_min = rea_df[rea_df['Risk Event Time'] == the_time]['ES CoD'].min()
#         data_dict[the_time].insert(4, es_cod_min)
#         es_cod_max = rea_df[rea_df['Risk Event Time'] == the_time]['ES CoD'].max()
#         data_dict[the_time].insert(5, es_cod_max)
#         es_cod_median = rea_df[rea_df['Risk Event Time'] == the_time]['ES CoD'].median()
#         data_dict[the_time].insert(6, es_cod_median)
#         es_cod_posmean = np.mean(
#             list(filter(lambda x: x > 0, rea_df[rea_df['Risk Event Time'] == the_time]['ES CoD'].
#                         values)))
#         data_dict[the_time].insert(7, es_cod_posmean)
#         es_cod_negmean = np.mean(
#             list(filter(lambda x: x < 0, rea_df[rea_df['Risk Event Time'] == the_time]['ES CoD'].
#                         values)))
#         data_dict[the_time].insert(8, es_cod_negmean)
#
#     data_dict['Metric'] = ['Mkt (ES) Total', 'Mkt (ES) Mean', 'Mkt (ES) Std', 'Mkt (ES) Est. Sharpe', 'Mkt (ES) Min',
#                            'Mkt (ES) Max', 'Mkt (ES) Median', 'Mkt (ES) Pos Mean', 'Mkt (ES) Neg Mean',
#                            'Mkt (ES), # Days', 'DYTC Total', 'DYTC, # Days']
#     return_df = pd.DataFrame.from_dict(data_dict)
#     return_df = return_df.loc[:,['Metric', 't-1d', 't', 't+1d']]
#     #return_df.set_index('Metric', inplace=True)
#
#     # create the data type dict
#     #data_type_dict = dict(zip(data_dict['Metric'], len(data_dict['Metric'])*[('numeric', None)]))
#     data_type_dict = {'Metric': ('text', None), 't-1d': ('numeric', None), 't': ('numeric', None),
#                       't+1d': ('numeric', None)}
#
#     return return_df, data_type_dict


# def create_integrity_df(df_ms, trade_plans, final_signals=None, prelim_signals=None):
#
#     if final_signals is not None:
#         clean_final_signals = [fs for fs in final_signals if 'Trade Plan' not in fs['name']]
#
#     df9_cols = ['Date', 'Bias', 'DYTC Units']  # 'Trade Plan Link', 'Latest Signal Publish Date/Time', 'Integrity?']
#     integrity_df = df_ms['Integrity Data']
#     integrity_df = integrity_df.apply(integrity_vectorized_helper,
#                                       trade_plans=trade_plans,
#                                       final_signals=clean_final_signals,
#                                       prelim_signals=prelim_signals,
#                                       axis=1)
#     integrity_df.reset_index(drop=True, inplace=True)
#     return integrity_df


# def integrity_vectorized_helper(row, trade_plans, final_signals, prelim_signals):
#
#     manually_verified_dates = {date(2023, 5, 25): datetime(2023, 5, 24, 11, 16),
#                                date(2023, 5, 16): datetime(2023, 5, 15, 10, 59),
#                                date(2023, 4, 25): datetime(2023, 4, 24, 9, 28),
#                                date(2023, 5, 18): datetime(2023, 5, 17, 7, 57)}
#     verified_patreon_final_signal_post = {date(2023, 5, 19): datetime(2023, 5, 18, 4, 41)}
#
#     the_date = row['Date']
#     the_tp = [tp for tp in trade_plans if the_date == datetime.strptime(tp['name'].split('-')[1].strip(),
#                                                                         "%m/%d/%Y").date()]
#     the_fs = [fs for fs in final_signals if the_date == datetime.strptime(fs['name'].split('-')[1].strip(),
#                                                                         "%m/%d/%Y").date()]
#     the_ps = [ps for ps in prelim_signals if the_date == datetime.strptime(ps['name'].split('-')[1].strip(),
#                                                                         "%m/%d/%Y").date()]
#     if len(the_fs) != 0:
#         the_fs_weblink = "[Final Signal Doc](" + the_fs[0]['webViewLink'] + ")"
#     else:
#         the_fs_weblink = ''
#     if len(the_tp) != 0:
#         the_tp_weblink = "[Trade Plan Doc](" + the_tp[0]['webViewLink'] + ")"
#     else:
#         the_tp_weblink = ''
#     if len(the_ps) != 0:
#         the_ps_weblink = "[Prelim Signal Doc](" + the_ps[0]['webViewLink'] + ")"
#     else:
#         the_ps_weblink = ''
#     if len(the_tp) == 0 and row['Bias'] == 'F':
#         row['Trade Plan Publish Date/Time (CST)'] = "No Signal"
#         row['Integrity?'] = 'No Signal'
#         row['Trade Plan Link'] = the_tp_weblink
#         row['Final Signal Link'] = the_fs_weblink
#         row['Prelim Signal Link'] = the_ps_weblink
#         return row
#     if len(the_tp) != 0:
#         last_modified = the_tp[0]['modifiedTime']
#         create_time = the_tp[0]['createdTime']
#         if 'sharedWithMeTime' in the_tp[0].keys():
#             shared_with_me_time = the_tp[0]['sharedWithMeTime']
#             shared_with_me_datetime = datetime.strptime(shared_with_me_time, '%Y-%m-%dT%H:%M:%S.%fZ')
#             cst_shared_datetime = shared_with_me_datetime - timedelta(hours=5)
#         else:
#             shared_with_me_time = None
#
#         last_modified_datetime = datetime.strptime(last_modified, '%Y-%m-%dT%H:%M:%S.%fZ')
#         created_datetime = datetime.strptime(create_time, '%Y-%m-%dT%H:%M:%S.%fZ')
#         cst_mod_datetime = last_modified_datetime - timedelta(hours=5)
#         cst_create_datetime = created_datetime - timedelta(hours=5)
#
#         if the_date in manually_verified_dates.keys():
#             cst_mod_datetime = manually_verified_dates[the_date]
#             shared_with_me_time = None
#         elif the_date in verified_patreon_final_signal_post.keys():
#             cst_mod_datetime = verified_patreon_final_signal_post[the_date]
#             shared_with_me_time = None
#
#         if shared_with_me_time is not None:
#             if cst_mod_datetime.replace(microsecond=0) == cst_shared_datetime.replace(microsecond=0):
#                 # use create time instead
#                 row['Trade Plan Publish Date/Time (CST)'] = cst_create_datetime
#             else:
#                 row['Trade Plan Publish Date/Time (CST)'] = cst_mod_datetime
#         else:
#             row['Trade Plan Publish Date/Time (CST)'] = cst_mod_datetime
#         if row['Trade Plan Publish Date/Time (CST)'] > datetime.combine(the_date, time(8, 29, 59)):
#             row['Integrity?'] = 'Unverified'
#         else:
#             row['Integrity?'] = 'Verified'
#         row['Trade Plan Link'] = the_tp_weblink
#     row['Final Signal Link'] = the_fs_weblink
#     row['Prelim Signal Link'] = the_ps_weblink
#
#     return row


# def create_accuracy_df(df):
#
#     within_risk_settle_hr = len(df[df['Within Risk @ Settle?'] == 1]) / len(df[df['Within Risk @ Settle?'] != 0])
#     within_risk_extrema_hr = len(df[df['Within Risk @ Extrema?'] == 1]) / len(df[df['Within Risk @ Extrema?'] != 0])
#     long_always_hr = len(df[df['ES CoD'] > 0]) / len(df)
#     dytc_model_hr = len(df[(df['DYTC Model CoD'] > 0) | (df['DYTC Model CoD'] == 0) & (df['ES CoD'] < 0)]) / len(df)
#     fundamental_model_long_hr = len(df[((df['Fundamental Model'] >= 0) & (df['Fundamental Model CoD'] > 0)) |
#                                        ((df['Fundamental Model CoD'] == 0) & (df['ES CoD'] < 0))]) / \
#                                 len(df[df['Fundamental Model'] >= 0])
#     fundamental_model_short_hr = len(df[(df['Fundamental Model'] < 0) & (df['Fundamental Model CoD'] > 0)]) / \
#                                  len(df[(df['Fundamental Model'] < 0) | ((df['Fundamental Model'] == 0) &
#                                                                          (df['ES CoD'] < 0))])
#     fundamental_model_total_hr = (len(df[(df['Fundamental Model'] != 0) & (df['Fundamental Model CoD'] > 0)]) + \
#                                   len(df[(df['Fundamental Model'] == 0) & (df['ES CoD'] < 0)])) / \
#                                  len(df['Fundamental Model'])
#
#     perc_days_dytc_model_profitable = len(df[df['DYTC Model Total'] > 0]) / len(df)
#     perc_days_mkt_profitable = len(df[df['Long Always'] > 0]) / len(df)
#     perc_days_fundamental_model_profitable = len(df[df['Fundamental Model Total'] > 0]) / len(df)
#
#     # are we using leverage well or not?
#     # dytc_leverage_effectiveness_ratio = (df[df['DYTC Units'] > 1]['Model CoD'].sum() - \
#     #                                    df[df['DYTC Units'] > 1]['ES CoD'].sum()) / \
#     #                                    (df[df['DYTC Units'] == 1]['Model CoD'].sum() - \
#     #                                    df[df['DYTC Units'] == 1]['ES CoD'].sum())
#     data_dict = {"Inside Risk @ Settle": [within_risk_settle_hr, 'DYTC'],
#                  "Inside Risk @ Extremes": [within_risk_extrema_hr, 'DYTC'],
#                  "Market Hit Rate": [long_always_hr, 'Long Always'],
#                  "DYTC Model Hit Rate": [dytc_model_hr, 'DYTC'],
#                  "Fundamental Model Hit Rate": [fundamental_model_total_hr, 'Fundamental'],
#                  "Fundamental Model, Long Hit Rate": [fundamental_model_long_hr, 'Fundamental'],
#                  "Fundamental Model, Short Hit Rate": [fundamental_model_short_hr, 'Fundamental'],
#                  "% Days DYTC in Black": [perc_days_dytc_model_profitable, 'DYTC'],
#                  "% Days Long Always in Black": [perc_days_mkt_profitable, 'Long Always'],
#                  "% Days Fundamental Model in Black": [perc_days_fundamental_model_profitable, 'Fundamental']}
#     bar_plot_df = pd.DataFrame(pd.Series(data_dict))
#     temp_df = pd.DataFrame(bar_plot_df[0].to_list(), columns=['Rate (%)', 'Model Desc'])
#     temp_df.index = bar_plot_df.index
#     return temp_df


# Rates


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
########### EQUITIES ###############
####################################

####################################
# GET FUNCTIONS
####################################

# GETTING S&P 500 constituents from Wikipedia - Save to csv
# def get_spx_cons(csv_path):
#     '''
#     Extract S&P 500 companies from wikipedia and store tickers and Sectors / Industries as df
#     Then store as csv.
#     '''
#     URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
#     df = pd.read_html(URL)[0]
#     df['Symbol'] = df['Symbol'].str.replace('.', '-')
#     df = df.drop(['Headquarters Location', 'Date added', 'CIK', 'Founded'], axis=1)
#     df = df.sort_values(by=['GICS Sector', 'GICS Sub-Industry'])
#     df = df.set_index('Symbol')
#     df.dropna(inplace=True)
#     return df.to_csv(csv_path)


# GETTING S&P prices from yfinance - Save to csv
# def get_prices(df):
#     '''
#     Dowload prices from yfinance from a list of tickers. returns df of prices written to a csv
#     '''
#     # local_path linked to drive via sinology
#     local_path = '/Users/chloeguillaume/SynologyDrive/Google Drive/DATA_PUBLIC/us_markets_dash_data/spx.csv'
#     # url public on google drive
#     file_id = '1SoheVoh79lEo5HhVxR_p_XdLewRAgWdh'
#     url_open = f'https://drive.google.com/uc?id={file_id}&export=download'
#     url_save = f'https://drive.google.com/u/0/uc?id={file_id}&export=download'
#
#     # file = pd.read_csv(local_path)
#
#     tickers_list = df.index.tolist()
#     start = '2020-12-31'
#     prices_df = yf.download(tickers_list, start=start, interval='1d', )
#     file = prices_df['Adj Close']
#
#     return file.to_csv(local_path)


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
#
#
# # load S&P 500 weights from IVV ETF stored in csv
def load_wiki_cons(csv_path):
    '''
    Load tickers, sectors, industries etc. from wiki csv file
    => df
    '''
    df = pd.read_csv(csv_path)
    df = df.set_index('Symbol')
    return df
#
#
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
#
#
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
#
#
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
#
#
# ####################################
# # FEATURE ENGINEERING
# ####################################
def join_dfs(df, df_IVV):
    df = df.join(df_IVV['Weight (%)'])
    df.sort_values(by='Weight (%)', inplace=True, ascending=False)
    df = df.rename(columns={'GICS Sector': 'Sector', 'GICS Sub-Industry': 'Sub-Industry', 'Weight (%)': 'Weight'})
    df.dropna(inplace=True)
    return df
