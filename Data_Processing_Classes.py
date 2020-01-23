'''
    author: Shlomo Amor
    last updated: 23/01/2020
    python version: 3.7.3
'''

from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import pandas as pd
import urllib3
from openpyxl import load_workbook
import re
import csv
from contextlib import closing

class getPricesByTicker:

    # Constructor receives  name of stock and date for required information
    def __init__(self, name, date, openingPrice = None, closingPrice= None):
        self.name = name
        self.date = date
        self.betaValue = 0
        self.dataframe = None
        if openingPrice is None and closingPrice is None:
            self.openingPrice = None
            self.closingPrice = None
        else:
            self.openingPrice = openingPrice
            self.closingPrice = closingPrice
        self.todaysTimeStamp = str(datetime.timestamp(datetime.now())).split('.', 1)[0]

    # Upon creating a instance of getPrices setValues sets all values
    def setValues(self):
        self.parseDateToYahoo()
        # Getting time stamp for the date
        timeStamp = self.getTimeStamp(self.date)
        # Getting the timestamp for the next day of the date
        timeStampForNextDay = self.getTimeStampExtraDay(self.date)
        # Creating a dataframe with single row of data of info from yahoo for the given date
        self.dataframe = self.createtableOfInfo(self.name, timeStamp, timeStampForNextDay)
        dateToString = self.parseDateFromNumberToString(self.date)
        self.openingPrice = self.getOpenPrice(dateToString)
        self.closingPrice = self.getClosePrice(dateToString)
        self.getBetaValue()

    # Converts a date in the format DD/MM/YYYY to a timestamp
    def getTimeStamp(self,date):
        startingDateParsed = date + " 00:00:00"
        datetimeObject = datetime.strptime(startingDateParsed, '%d/%m/%y %H:%M:%S')
        timeStamp = str(datetime.timestamp(datetimeObject)).split('.', 1)[0]
        return timeStamp

    # Converts a date in the format DD/MM/YYYY to a timestamp and adding an extra day
    def getTimeStampExtraDay(self,date):
        startingDateParsed = date + " 00:00:00"
        datetimeObject = datetime.strptime(startingDateParsed, '%d/%m/%y %H:%M:%S')
        timeStamp = str(datetime.timestamp(datetimeObject+ timedelta(days=1))).split('.', 1)[0]
        return timeStamp

    # Scrape Yahoo finance and create a dataframe with values between given dates
    def createtableOfInfo(self, name, startingTimeStamp, endingtimeStamp):
        try:
            url = "https://finance.yahoo.com/quote/"+name+"/history?period1="+str(startingTimeStamp)+"&period2="+str(endingtimeStamp)+\
                   "&interval=1d&filter=history&frequency=1d"
            request = requests.get(url)
            soup = BeautifulSoup(request.text, "html.parser")

            # Creating a list of titles
            tableTitles = soup.find('tr', {'class': 'C($tertiaryColor) Fz(xs) Ta(end)'}).findAll('span')
            listOfTitles = []
            for titles in tableTitles:
               listOfTitles.append(titles.text)
            dataframe = pd.DataFrame(columns=listOfTitles)

            # Creating a list per row
            rowList = []
            tableRows = soup.findAll('tr', {'class': 'BdT Bdc($seperatorColor) Ta(end) Fz(s) Whs(nw)'})
            for row in tableRows:
                rowEntries = row.findAll('td')
                # For a given row populate the info in a row list
                for index, entry in enumerate(rowEntries):
                    rowList.append(entry.text)
                # Add row to dataframe
                try:
                    dataframe = dataframe.append(pd.Series(rowList, index=listOfTitles), ignore_index=True)
                except:
                    pass
                rowList = []
        except:
                dataframe = pd.DataFrame([])
        return dataframe

    # Scraping from Yahoo finance the betaValue
    def getBetaValue(self):
        url = "https://finance.yahoo.com/quote/" + self.name
        request = requests.get(url)
        soup = BeautifulSoup(request.text, "html.parser")
        # Creating a list of td tags
        tableTitles = soup.findAll('td')
        for td in tableTitles:
            if "BETA_5Y-value" in str(td):
                try:
                    self.betaValue = float(td.text)
                except:
                    self.betaValue = 0

    # Extract Opening price for a given date from dataframe.
    def getOpenPrice(self, date):
        try:
            try:
                self.openingPrice = self.dataframe.loc[self.dataframe['Date'] == date, 'Open'].tolist()[0]
                return self.openingPrice
            except:
                self.openingPrice = self.dataframe.iloc[-1]['Open']
                return self.openingPrice
        except:
            self.openingPrice = self.getFromDataComCSV()
            #print("Got price from datacom")
            return self.openingPrice

    # Extract Closing price for a given date from dataframe.
    def getClosePrice(self, date):
        try:
            try:
                self.closingPrice = self.dataframe.loc[self.dataframe['Date'] == date, 'Close*'].tolist()[0]
                return self.closingPrice
            except:
                self.closingPrice = self.dataframe.iloc[-1]['Close*']
                return self.closingPrice
        except:
            self.closingPrice = self.getFromDataComCSV()
            #print("Got price from datacom.")
            return self.openingPrice

    # Converts given date in this format 10/08/2019 to Aug 10, 2019
    def parseDateFromNumberToString(self, date):
        day = date[:2]
        monthString = date[3:5]
        monthSwitcher = {
            "01": 'Jan',
            "02": "Feb",
            '03': "Mar",
            '04': "Apr",
            '05': "May",
            '06': "Jun",
            '07': "Jul",
            '08': "Aug",
            '09': "Sep",
            '10': "Oct",
            '11': "Nov",
            '12': "Dec",
        }
        month = monthSwitcher[monthString]
        year = date[6:]
        return month + " " + day + ", 20" + year

    # Changes the given date to the format yahoo finance accepts
    def parseDateToYahoo(self):
        day = self.date[:2]
        month = self.date[3:5]
        year = self.date[8:]
        self.date = day+"/"+month+"/"+year

    # Search the datacom for a closing price for a ticker and date (XLSX)
    def getFromDataCom(self, date):
        print("In datacom search")
        if len(date) == 8:
            newdate = date[0:6]+"20"+date[6:]
        else:
            newdate = date
        # Opening datacom for reading
        filePath = "C:\\Users\\shlom\\Dropbox\\datcommodcurr.xlsx"
        wb = load_workbook(filePath, read_only=True)
        ws = wb['Sheet1']

        colIndex = 0
        rowIndex = 0
        # Getting ticker col
        for row in ws.rows:
            for cell in row:
                if cell.value == self.name:
                    colIndex = cell.column
                    print("Found ticker name", str(cell.value))
                    break
            else:
                continue
            break

        found = False
        # Getting the row by checking what our date is
        for row in ws.rows:
            if not found:
                for index,cell in enumerate(row):
                    if not found:
                        if index == 0:
                            strCell = str(cell.value)
                            try:
                                pattern = re.compile("(\d{4})-(\d{2})-(\d{2})")
                                match = pattern.match(strCell)
                                date = match.group(3)+"/"+match.group(2)+"/"+match.group(1)
                                if date == newdate:
                                    rowIndex = cell.row
                                    found = True
                                    print("Found date col", str(cell.value))

                            except:
                                continue
                            finally:
                                break
            else:
                break

        found = False
        # Getting the closing price at the index
        for i,row in enumerate(ws.rows):
            if not found:
                for index,cell in enumerate(row):
                    if not found:
                        if index == colIndex-1 and i == rowIndex-1:
                            price = cell.value
                            found = True
                            print("Found closing Price: ", str(cell.value))
                            break
            else:
                break
        return price

    # Search the datacom for a closing price for a ticker and date (CSV)
    def getFromDataComCSV(self):
        if len(self.date) == 8:
            newdate = self.date[0:6]+"20"+self.date[6:]
        else:
            newdate = self.date

        filePath = "C:\\Users\\shlom\\Dropbox\\datcommodcurr.csv"
        found = False
        price = 0
        try:
            with open(filePath) as datacom_file:
                csv_reader = csv.reader(datacom_file, delimiter=',')
                data = [x for x in csv_reader]
                for i in range(len(data)):
                    if data[i][0] == newdate and found:
                        price = data[i][colIdx]
                        break
                    for j in range(len(data[i])):
                        if data[i][j] == self.name:
                            colIdx = j
                            found = True
                if not found:
                    print("Could not find value in datacom. Setting price to 0")
                    return 0
        except:
            print("Error. File could not be opened.")
        return price

    # To string of the object
    def toString(self):
        print("Name        :", self.name)
        print("Open Price  :", self.openingPrice)
        print("Close Price :", self.closingPrice)
        print("Beta Value  :", self.betaValue)
        print("Date        :", self.date)

#TESTS
# test = getPricesByTicker("LUMI.TA", "08/01/2020")
# test.setValues()
# test.toString()
#test.getBetaValue()


