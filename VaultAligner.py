import threading
import time
import gspread
import httplib2
import os
import time
import concurrent.futures
import traceback

from oauth2client.service_account import ServiceAccountCredentials
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

authorization_thread_lock = threading.Lock()
data_thread_lock = threading.Lock()

save_file = open("failedCells.txt", "w+")
x = 0

failed_cells = []

def authenticate_with_sheets():
    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
    client = gspread.authorize(creds)

    http = creds.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

    spread_sheet = client.open('CME Dairy Futures History')

    authenticate_with_sheets.organized_sheet = spread_sheet.worksheet('DRY WHEY ORGANIZED')
    authenticate_with_sheets.dry_whey_sheet = spread_sheet.worksheet('DRY WHEY NO APO')

def get_date(cell_col, cell_row, sheet):
    cell_val = sheet.cell(cell_row, cell_col).value

    year = sheet.cell(3, cell_col).value

    if cell_col <= 12:
        day_month = sheet.cell(cell_row, 1).value
    elif cell_col >= 15 and cell_col <= 25:
        day_month = sheet.cell(cell_row, 14).value
    elif cell_col >= 28 and cell_col <= 38:
        day_month = sheet.cell(cell_row, 27).value
    elif cell_col >= 41 and cell_col <= 52:
        day_month = sheet.cell(cell_row, 40).value
    elif cell_col >= 55 and cell_col <= 66:
        day_month = sheet.cell(cell_row, 54).value
    elif cell_col >= 69 and cell_col <= 80:
        day_month = sheet.cell(cell_row, 68).value
    elif cell_col >= 83 and cell_col <= 94:
        day_month = sheet.cell(cell_row, 82).value
    elif cell_col >= 97 and cell_col <= 108:
        day_month = sheet.cell(cell_row, 96).value
    elif cell_col >= 111 and cell_col <= 122:
        day_month = sheet.cell(cell_row, 110).value
    elif cell_col >= 125 and cell_col <= 136:
        day_month = sheet.cell(cell_row, 124).value
    elif cell_col >= 139 and cell_col <= 150:
        day_month = sheet.cell(cell_row, 138).value
    elif cell_col >= 153 and cell_col <= 163:
        day_month = sheet.cell(cell_row, 152).value

    date = str(day_month) + "-" + str(year)
    return_val_with_date = date + ", " + cell_val

    return date

def get_contract(cell_col, cell_row, sheet):

    contract = "N/A"
    column = None

    if cell_col <= 12:
        contract = "jan"
        column = 2
    elif cell_col >= 15 and cell_col <= 25:
        contract = "feb"
        column = 3
    elif cell_col >= 28 and cell_col <= 38:
        contract = "mar"
        column = 4
    elif cell_col >= 41 and cell_col <= 52:
        contract = "apr"
        column = 5
    elif cell_col >= 55 and cell_col <= 66:
        contract = "may"
        column = 6
    elif cell_col >= 69 and cell_col <= 80:
        contract = "jun"
        column = 7
    elif cell_col >= 83 and cell_col <= 94:
        contract = "jul"
        column = 8
    elif cell_col >= 97 and cell_col <= 108:
        contract = "aug"
        column = 9
    elif cell_col >= 111 and cell_col <= 122:
        contract = "sep"
        column = 10
    elif cell_col >= 125 and cell_col <= 136:
        contract = "oct"
        column = 11
    elif cell_col >= 139 and cell_col <= 150:
        contract = "nov"
        column = 12
    elif cell_col >= 153 and cell_col <= 163:
        contract = "dec"
        column = 13

    return column

def align_data(i):

    try:
        #with data_thread_lock:
        contract = get_contract(i.col, i.row, authenticate_with_sheets.dry_whey_sheet)
        if not i.value == "" and not contract == 0:
            new_col = contract
            new_date = get_date(i.col, i.row, authenticate_with_sheets.dry_whey_sheet)

        else:
            print("\nWorker: \t{}\nEmpty cell skipped: ".format(threading.current_thread().name) + str(i.col) + " " + str(i.row) + "\n")
            return

    except Exception:
        print ("Got an error.")
        traceback.print_exc()
        save_file.write(str(i) + "\n")
        failed_cells.append(str(i))

    else:
        try:
            corresponding_cell = authenticate_with_sheets.organized_sheet.find(new_date)

        except Exception:
            print("\n\nWorker: \t{}\nError searching sheet. Gaining new authentication.\n\n\n".format(threading.current_thread().name))
            #with authorization_thread_lock:
            authenticate_with_sheets()
            save_file.write(str(i) + "\n")
            failed_cells.append(str(i))

        else:
            new_row = corresponding_cell.row

        print("Worker: \t{}\nColumn: \t{}\nRow: \t\t{}\nNew Date: \t{}\nNew Contract: \t{}\nNew Value: \t{}\n".format(threading.current_thread().name, str(new_col), str(new_row), str(new_date), str(contract), str(i.value)))

        try:
            authenticate_with_sheets.organized_sheet.update_cell(new_row, new_col, i.value)

        except Exception:
            print("\n\nWorker: \t{}\nError updating sheet. Gaining new authentication.\n\n\n".format(threading.current_thread().name))
            #with authorization_thread_lock:
            authenticate_with_sheets()
            save_file.write(str(i) + "\n")
            failed_cells.append(str(i))

def execute():
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        result = executor.map(align_data, authenticate_with_sheets.dry_whey_sheet.range('B121:FG366'))
