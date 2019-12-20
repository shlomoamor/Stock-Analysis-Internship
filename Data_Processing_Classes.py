from bs4 import BeautifulSoup
import requests
from datetime import datetime
import time
import pandas as pd

class getPricesByTicker:

    # Constructor recieves  name of stock and date for required information
    def __init__(self, name, date):
        self.name = name
        self.date = date
        self.dataframe = ""
        self.openingPrice = ""
        self.closingPrice = 0
        self.todaysTimeStamp = str(datetime.timestamp(datetime.now())).split('.', 1)[0]

    # Upon creating a instance of getPrices setValues sets all values
    def setValues(self):
        timeStamp = self.getTimeStamp(self.date)
        self.dataframe = self.createtableOfInfo(self.name, timeStamp, self.todaysTimeStamp)
        #print(self.dataframe)
        dateToString = self.parseDateFromNumberToString(self.date)
        self.getOpenPrice(dateToString)
        self.getClosePrice(dateToString)

    # Converts a date in the format DD/MM/YYYY to a timestamp
    def getTimeStamp(self,date):
        startingDateParsed = date + " 00:00:00"
        datetimeObject = datetime.strptime(startingDateParsed, '%d/%m/%y %H:%M:%S')
        timeStamp = str(datetime.timestamp(datetimeObject)).split('.', 1)[0]
        return timeStamp

    # Scrape Yahoo finance and create a dataframe with values between given dates
    def createtableOfInfo(self, name, startingTimeStamp, endingtimeStamp):
        url = "https://finance.yahoo.com/quote/TSEM.TA/history?period1="+str(startingTimeStamp)+"&period2="+str(endingtimeStamp)+\
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
            dataframe = dataframe.append(pd.Series(rowList, index=listOfTitles), ignore_index=True)
            rowList = []
        return dataframe

    # Extract Opening price for a given date from dataframe.
    def getOpenPrice(self, date):
        try:
            self.openingPrice = self.dataframe.loc[dataframe['Date'] == date, 'Open'].tolist()[0]
        except:
            self.openingPrice = self.dataframe.iloc[-1]['Open']

    # Extract Closing price for a given date from dataframe.
    def getClosePrice(self, date):
        try:
            self.closingPrice = self.dataframe.loc[dataframe['Date'] == date, 'Close*'].tolist()[0]
        except:
            self.closingPrice = self.dataframe.iloc[-1]['Close*']

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

# TESTS
# t = getPricesByTicker("TSEM.TA", "03/11/19")
# t.setValues()
#
# print(t.openingPrice)
# print(t.closingPrice)