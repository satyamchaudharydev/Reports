from google.oauth2.service_account import Credentials
from googleapiclient import discovery
creds = Credentials.from_service_account_file('./auth.json')
service = discovery.build('sheets', 'v4', credentials=creds)

spreadsheet_id = '1JE4dFUycVV1KevVaJhej92JU95fANGyFG0aBJde9_e0'
range_ = 'School Overview Data'
def updateSheet(data):
    body = {
    'values': data
    }
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id, range=range_
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_,
        valueInputOption='RAW', body=body
    ).execute()
