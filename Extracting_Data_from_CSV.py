import csv
from datetime import datetime, timedelta
import pandas as pd
import enum
import numpy as np

class getInfoFromForecast:

    # Constructor receiving file name, date of forecast, required horizon, and region for testing purposes
    def __init__(self, forecast_csv, date, horizon, region):
        self.fileName = forecast_csv
        self.openingdate = date
        self.closingDate = ""
        self.horizon = horizon
        self.region =region
        self.listOfStocks = []
        self.listOfStocksHorizon = []
        self.forecastDataframe = pd.DataFrame()
        self.forecastDataframeHorizon = pd.DataFrame()

    # Upon creating a instance of getInfoFromForecast setValues sets all values
    def setValues(self):
        parsedDate = self.convertGivenDateToFileDate()
        if self.checkValidityOfFile(parsedDate):
            self.setClosingDate()
            self.createStockDictionaries()
            self.createStockDictionariesForHorizon()
            self.convertListToDataFrame(self.listOfStocks)
            self.convertListToDataFrame(self.listOfStocksHorizon)
            self.updateFrequencyCol()

    # Check if the given file is a correct file
    def checkValidityOfFile(self,date):
        isCorrectFile = True
        region = self.region
        filename = "IKForecast_" + region + "_flat_" + date + ".csv"
        # Check if the file matches the information provided
        if filename != self.fileName:
            print("Error! Incorrect file given. \nGiven filename is: ", self.fileName, "\nExpected filename: ",
                  filename)
            isCorrectFile = False
        return isCorrectFile

    # Opening the file and creating a list of dictionaries of stocks
    def createStockDictionaries(self):
        try:
            with open(self.fileName) as forecast_file:
                csv_reader = csv.reader(forecast_file, delimiter=',')
                for row in csv_reader:
                    if row[1] == "3d" or row[1] == "7d" or row[1] == "14d" \
                            or row[1] == "30d" or row[1] == "90d" or row[1] == "year":
                        tickerDict = self.convertTickerListToDict(row)
                        self.listOfStocks.append(tickerDict)
                self.addCellColourtoTicker(self.listOfStocks)
        except:
            print("Error opening file. File does not exist in directory")

    # Opening the file and creating a list of dictionaries of stocks for the given horizon
    def createStockDictionariesForHorizon(self):
        try:
            with open(self.fileName) as forecast_file:
                csv_reader = csv.reader(forecast_file, delimiter=',')
                for row in csv_reader:
                    if row[1] == self.horizon:
                        tickerDict = self.convertTickerListToDict(row)
                        self.listOfStocksHorizon.append(tickerDict)
                self.addCellColourtoTicker(self.listOfStocksHorizon)
        except:
            print("Error opening file. File does not exist in directory")

    # Converts the list of dictionaries to a dataframe
    def convertListToDataFrame(self, lst):
        if lst == self.listOfStocksHorizon:
            self.forecastDataframeHorizon = pd.DataFrame(lst)
        elif lst == self.listOfStocks:
            self.forecastDataframe = pd.DataFrame(lst)

    # Receives a list of ticker Dictionaries and adds the appropriate tags for the ticker
    def addCellColourtoTicker(self, lst):
        try:
            for ticker in lst:
                if ticker['Signal'] >= 10:
                    ticker['Cell colour'] = "Dark Green"
                elif ticker['Signal'] >= 0:
                    ticker['Cell colour'] = "Light Green"
                elif ticker['Signal'] < -10:
                    ticker['Cell colour'] = "Dark Red"
                else:
                    ticker['Cell colour'] = "Light red"
        except:
            print("Error! List of stocks has not been created yet.")

    # Converts a row from a file into a dictionary
    def convertTickerListToDict(self,lst):
        if len(lst) != 4:
            return {}
        else:
            tickerDict = {"Name":lst[0], "Horizon":lst[1], "Predictability":float(lst[2]), "Signal": float(lst[3]),
                          "Frequency":0}
            return tickerDict

    # Changing our opening date to a Number value
    def parseDatetoNumber(self):
        day = self.openingdate[:2]
        monthString = self.openingdate[3:6]
        monthSwitcher = {
            'Jan': "01",
            'Feb': "02",
            'Mar': "03",
            'Apr': "04",
            'May': "05",
            'Jun': "06",
            'Jul': "07",
            'Aug': "08",
            'Sep': "09",
            'Oct': "10",
            'Nov': "11",
            'Dec': "12",
        }
        month = monthSwitcher[monthString]
        year = self.openingdate[7:]
        newDate = day + " " + month + " " + year
        self.openingdate = newDate

    # Converts date to file date
    def convertGivenDateToFileDate(self):
        day = self.openingdate[:2]
        monthNum = self.openingdate[3:5]
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
        month = monthSwitcher[monthNum]
        year = "20"+self.openingdate[8:]
        self.openingdate = day + "_"+month+"_"+year
        return day + "_"+month+"_"+year

    # Setting the closinf date to the Opening Date + the horizon
    def setClosingDate(self):
        self.parseDatetoNumber()
        OpeningDateParsed = self.openingdate + " 00:00:00"
        datetimeObject = datetime.strptime(OpeningDateParsed, '%d %m %Y %H:%M:%S')
        # Now we add the horizon to get closing date
        if self.horizon != "year":
            try:
                self.closingDate = datetimeObject + timedelta(days=int(self.horizon.replace("d", "")))
            except ValueError:
                print("error")
        elif self.horizon == "year":
            try:
                self.closingDate = datetimeObject.replace(year=(datetimeObject.year + 1))
            except ValueError:
                self.closingDate = datetimeObject + (
                            datetime(datetimeObject.year + 1, 1, 1) - datetime(datetimeObject.year, 1, 1))
                print("Error setting closing date. Try again")

    # Prints a given dictionary keys:value
    def printDict(self, dict):
        for key in dict:
            if key == "Cell colour":
                print(key, ":", dict[key])
            else:
                print(key, ":", dict[key], end=", ")

    # Prints the list of stocks to the screen
    def toString(self):
        for ticker in self.listOfStocks:
            self.printDict(ticker)

    # Update the frequencies for each stock in the forecastdataframe
    def updateFrequencyCol(self):
        count = 0
        copy = self.forecastDataframe.copy()
        for row in copy.index:
            for t in copy.index:
                if copy['Name'][t] == copy['Name'][row]:
                    count+=1
            self.forecastDataframe['Frequency'][row] = count
            count = 0



#
# year = getInfoFromForecast("IKForecast_Israel_flat_01_Sep_2019.csv", "01/09/2019", "14d", "Israel")
# year.setValues()
# #year.toString()
# #year.updateFrequencyCol()
# print(year.forecastDataframeHorizon)
# print(year.forecastDataframe)
