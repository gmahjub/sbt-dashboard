from __future__ import print_function

import io
import os.path
import gspread
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


# If modifying these scopes, delete the file token.json.
#SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
SCOPES = ['https://www.googleapis.com/auth/drive']

# credentials location
credentials_location = "/Users/ghazymahjub/workspace/google_api_credentials.json"


def get_creds():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_location, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds


def read_file(fn, sheet_name):

    creds = get_creds()
    client = gspread.authorize(creds)
    worksheet = client.open(fn).worksheet(sheet_name)
    rows = worksheet.get_all_records()
    df = pd.DataFrame(rows)
    return df


def export_file(items, fn, format='text/csv'):

    real_file_id = [item['id'] for item in items if item['name'] == fn][0]
    creds = get_creds()
    try:
        service = build('drive', 'v3', credentials=creds)
        request = service.files().export_media(fileId=real_file_id, mimeType=format)

        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
    except HttpError as error:
        file = None

    return file.getvalue()


def return_files_on_drive():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = get_creds()

    try:
        service = build('drive', 'v3', credentials=creds)

        # Call the Drive v3 API
        results = service.files().list(
            pageSize=10, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])

        if not items:
            print('No files found.')
            return
        print('Files:')
        for item in items:
            print(u'{0} ({1})'.format(item['name'], item['id']))
        return items
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')


if __name__ == '__main__':
    items = return_files_on_drive()
    #export_file(items, 'Prod-Dashboard Data', format='text/csv')
    read_file('Prod-Dashboard Data', sheet_name='RealTime')