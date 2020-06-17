import pandas as pd
import numpy as np
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow,Flow
from google.auth.transport.requests import Request
import os
import pickle

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

#############################
# Reading data from sheet 1 #
#############################

# here enter the id of your google sheet
SAMPLE_SPREADSHEET_ID_input = '1lcAEQ-QzEOo04R9e6W1xC8Ml9CS5D2hty21HIfC-Scs'
# Sheet 1
SAMPLE_RANGE_NAME = 'A1:AA1000'

def main():
    global values_input, service
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES) # here enter the name of your downloaded JSON file
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result_input = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
                                range=SAMPLE_RANGE_NAME).execute()
    values_input = result_input.get('values', [])

    if not values_input and not values_expansion:
        print('No data found.')
    
    ipab_edited=pd.DataFrame(values_input[1:], columns=values_input[0])

    # return edited file
    return ipab_edited

# main()

# ipab_edited=pd.DataFrame(values_input[1:], columns=values_input[0])

##############################
# Editting data from sheet 1 #
##############################

# IPAB raw data

# csv_file = 'IPAB Data for processing.csv'

def edit_sheet1_data(csv_file, edited_file):
    # ipab_edited=pd.DataFrame(values_input[1:], columns=values_input[0])

    df = pd.read_csv(csv_file)
    df_cat_words = edited_file
    unique_words = df_cat_words['Word'].unique()

    category_word_list = []
    for index, row in df_cat_words.iterrows():
        category_word_list.append((row['Category'], row['Word']))

    column_names = ['CustomerID', 'Word', 'Category']
    customer_keywords = pd.DataFrame(columns = column_names)

    for row in range(len(df)):
        for category_word in category_word_list:
            if category_word[1] in df.iloc[row,0]:
                customer_keywords = customer_keywords.append({'CustomerID':row, 'Word':category_word[1], 'Category': category_word[0]}, ignore_index=True)

    # google sheets can only have a maximum of 500000 fields            
    customer_keywords = customer_keywords.head(16660)

    # return range of the sheets
    range_name = 'Sheet2!A1:D'+ str(len(customer_keywords)+1)
    return range_name, customer_keywords

# edit_sheet1_data(csv_file)

###################################################
# Inserting Editted data from sheet 1 into sheet 2#
###################################################

#change the range if needed
# NEW_RANGE_NAME = 'Sheet2!A1:D'+ str(len(customer_keywords)+1)

def Create_Service(client_secret_file, api_service_name, api_version, *scopes):
    global service
    SCOPES = [scope for scope in scopes[0]]
    #print(SCOPES)
    
    cred = None

    if os.path.exists('token_write.pickle'):
        with open('token_write.pickle', 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            cred = flow.run_local_server()

        with open('token_write.pickle', 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(api_service_name, api_version, credentials=cred)
        print(api_service_name, 'service created successfully')
        #return service
    except Exception as e:
        print(e)
        #return None
        
# change 'my_json_file.json' by your downloaded JSON file.
Create_Service('credentials.json', 'sheets', 'v4',['https://www.googleapis.com/auth/spreadsheets'])
    
def Export_Data_To_Sheets(new_sheets_range, customer_keywords):
    response_date = service.spreadsheets().values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
        valueInputOption='RAW',
        range=new_sheets_range,
        body=dict(
            majorDimension='ROWS',
            values=customer_keywords.T.reset_index().T.values.tolist())
    ).execute()
    print('Sheet successfully Updated')

def insert_data_to_sheets(new_sheets_range, customer_keywords):
    Create_Service('credentials.json', 'sheets', 'v4',['https://www.googleapis.com/auth/spreadsheets'])
    Export_Data_To_Sheets(new_sheets_range, customer_keywords)