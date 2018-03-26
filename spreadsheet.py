import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pprint
import httplib2
import os

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

#Gaining access to Sheets.
scope = ['https://spreadsheets.google.com/feeds']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)

http = creds.authorize(httplib2.Http())
discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

#Make console output look pretty.
pp = pprint.PrettyPrinter()

#Get entire Spreadsheet.
spread_sheet = client.open('CME Dairy Futures History')
spread_sheet_id = '1SU5H5ozn30lBCz-EzOdQBlDIGKu_7Bxe4ZjUzOUXtPw'

#Assign variables to desired sheets within spread_sheet.
organized_sheet = spread_sheet.worksheet('DRY WHEY ORGANIZED')
dry_whey_sheet = spread_sheet.worksheet('DRY WHEY NO APO')

data_range = 'DRY WHEY NO APO!A4:FG366'
result = service.spreadsheets().values().get(spreadsheetId=spread_sheet_id, range=data_range).execute()

#Assign desired data to a variable.
data = result.get('values', [])

#Print all data.
#print(data)

#Get the date of a cell given the row, the column, and the sheet desired.
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
#Get the contract of a cell given the row, the column, and the sheet desired.
def get_contract(cell_col, cell_row, sheet):

    contract = "N/A"

    if cell_col <= 12:
        contract = "jan"
    elif cell_col >= 15 and cell_col <= 25:
        contract = "feb"
    elif cell_col >= 28 and cell_col <= 38:
        contract = "mar"
    elif cell_col >= 41 and cell_col <= 52:
        contract = "apr"
    elif cell_col >= 55 and cell_col <= 66:
        contract = "may"
    elif cell_col >= 69 and cell_col <= 80:
        contract = "jun"
    elif cell_col >= 83 and cell_col <= 94:
        contract = "jul"
    elif cell_col >= 97 and cell_col <= 108:
        contract = "aug"
    elif cell_col >= 111 and cell_col <= 122:
        contract = "sep"
    elif cell_col >= 125 and cell_col <= 136:
        contract = "oct"
    elif cell_col >= 139 and cell_col <= 150:
        contract = "nov"
    elif cell_col >= 153 and cell_col <= 163:
        contract = "dec"

    return contract

#Function to align data in the respective organized sheet.
def align_data(data_sheet):

    save_file = open("failedCells.txt", "w+")
    x = 0

    #Place to store cells that error out. Rearrange by hand or find the issue.
    failed_cells = []

    for i in data_sheet.range('B59:FG366'):
        x += 1

        #Get the date and contract of each cell.
        try:
            contract = get_contract(i.col, i.row, data_sheet)
            #Skip blank cells.
            if not i.value == "" and not contract == "N/A":
                if contract == "jan":
                    new_col = 2
                elif contract == "feb":
                    new_col = 3
                elif contract == "mar":
                    new_col = 4
                elif contract == "apr":
                    new_col = 5
                elif contract == "may":
                    new_col = 6
                elif contract == "jun":
                    new_col = 7
                elif contract == "jul":
                    new_col = 8
                elif contract == "aug":
                    new_col = 9
                elif contract == "sep":
                    new_col = 10
                elif contract == "oct":
                    new_col = 11
                elif contract == "nov":
                    new_col = 12
                elif contract == "dec":
                    new_col = 13
                new_date = get_date(i.col, i.row, data_sheet)
            else:
                print("Empty cell skipped: " + str(i.col) + " " + str(i.row) + "\n")
                continue

        except Exception:
            print "Got an error."
            save_file.write(str(i) + "\n")
            failed_cells.append(str(i))
            continue
        else:
            print("iteration: " + str(x))
            print("New Date: " + str(new_date))
            print("New Contract: " + str(get_contract(i.col, i.row, data_sheet) + "\n"))

            corresponding_cell = organized_sheet.find(new_date)
            new_row = corresponding_cell.row

            print("Column: " + str(new_col) + ", " + "Row: " + str(new_row))
            print("Value: " + str(i.value))
            try:
                organized_sheet.update_cell(new_row, new_col, i.value)
            except Exception:
                print("\n\nRecieved 401 error. Gaining new authentication.\n\n")
                creds.authorize(httplib2.Http())
                save_file.write(str(i) + "\n")
                failed_cells.append(str(i))
                continue


align_data(dry_whey_sheet)
