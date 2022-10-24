import sys
import petl
import configparser
import requests
import datetime
import json
import decimal
import pypyodbc

# get data from config gile
config = configparser.ConfigParser()

try:
    config.read('ETLDemo.ini')
except Exception as e:
    print('Could not read config file:' + str(e))
    sys.exit()

# read settings from config file
startDate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
destServer = config['CONFIG']['server']
destDatabase = config['CONFIG']['database']

# request data from URL
try:
    BOCResponse = requests.get(url+startDate)
except Exception as e:
    print('Could not make request:' + str(e))
    sys.exit()

# initialize list of lists for data storage
BOCDates = []
BOCRates = []

# check response status and process BOC JSON object
if(BOCResponse.status_code == 200):
    BOCRaw = json.loads(BOCResponse.text)

    # extract observation data into column arrays
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'],'%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))

    # create petl table from column arrays and rename the columns
    exchangeRates = petl.fromcolumns([BOCDates, BOCRates], header=['date', 'rate'])

    # load expense document
    try: 
        expenses = petl.io.xlsx.fromxlsx('Expenses.xlsx', sheet='Github')
    except Exception as e:
        print('could not open expenses.xlsx:' + str(e))
        sys.exit()

    # join tables
    expenses = petl.outerjoin(exchangeRates, expenses, key='date')

    # fill down missing values
    expenses = petl.filldown(expenses, 'rate')

    # remove dates with no expenses
    expenses = petl.select(expenses, lambda rec: rec.USD != None)

    # add CDN column
    expenses = petl.addfield(expenses, 'CAD', lambda rec: decimal.Decimal(rec.USD) * rec.rate)

    # initialize database connection
    try:
        dbConnection = pypyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=etl_db;Trusted_Connection=yes;')
        # dbConnection = pymssql.connect(host='localhost', user='root', password='', database='etl_db')
        # dbConnection = MySQLdb.connect(host="localhost", user="root", passwd="password", db="etl")
    except Exception as e:
        print('could not connect to database:' + str(e))
        sys.exit()

    # populate Expenses database table
    try:
        petl.io.todb(expenses, dbConnection, 'Expenses')
    except Exception as e:
        print('Could not write to database:' + str(e))
