'''
    author: Shlomo Amor
    last updated: 23/01/2020
    python version: 3.7.3
'''
from Extracting_Data_from_CSV import getInfoFromForecast
from Data_Processing_Classes import getPricesByTicker
import csv
import pandas as pd


class userBasedRules(getInfoFromForecast):
    # Constructor receiving file name, date of forecast, required horizon, region and criteria.csv for testing purposes
    def __init__(self, forecast_csv, date, horizon, region, userRulesFilename, priorityLevel, strategyFile):
        getInfoFromForecast.__init__(self, forecast_csv[0], date, horizon, region)
        self.userRulesFilename = userRulesFilename
        self.priorityLevel = priorityLevel
        self.prevDataFile = "User_Rules_Files/Prev_calc_data_" + str(self.horizon) + "_" + \
                            self.openingdate.replace("/", "_") + ".csv"
        self.strategyFile = strategyFile

    # Upon creating a instance of userBasedRules setValues sets all values
    def setValues(self):
        # Setting visability settings for the dataframes
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', -1)

        parsedDate = self.convertGivenDateToFileDate()

        # Creating a dictionary of horizons to dataframes
        if getInfoFromForecast.checkValidityOfFile(self, parsedDate):
            self.lstOfHorizonDf = []
            lst = ["3d", "7d", "14d", "30d", "90d", "year"]
            for d in lst:
                lstForHozion = getInfoFromForecast.createStockDictionariesForHorizon(self, d)
                dfForHozion = getInfoFromForecast.convertListToDataFrame(self, lstForHozion)
                dictDF = {d: dfForHozion}
                self.lstOfHorizonDf.append(dictDF)
            for h in self.lstOfHorizonDf:
                for i in h:
                    if i == self.horizon:
                        self.forecastDataframeHorizon = h[i]

            # Get user Criteria from CSV file
            self.getUserCriteria(self.horizon)
            # Filter forecast according to criteria, default start at priority level 5
            self.filterAccordingToCriterion(self.forecastDataframeHorizon, self.priorityLevel)
            # Adding the beta col
            self.addBetaColumn(self.filteredForecast, self.openingdate)
            # Calc weights for dataframe
            self.calcWeightsForFilteredDataframe(self.filteredForecast)
            # Calc return for each ticker in filtered forecast
            # self.addReturnCol(self.forecastDataframeHorizon,self.filteredForecast, self.horizon)
            # # Calculate the returns for the whole portfolio
            # self.calcRateOfReturnForPortfolio(self.filteredForecast)
            # # Extracting previous results to a csv file for future use
            # self.extractToStorageFile()

    # Check that we received a valid file from the client
    def checkValidityOfCriteriaFile(self, files):
        isCorrectFile = True
        filename = "User_Filter_Criteria_" + str(self.horizon) + ".csv"
        # Check if the file matches the information provided
        if filename not in files:
            print("Error! Incorrect file given. \nGiven filename is: ", self.userRulesFilename, "\nExpected filename: ",
                  filename)
            isCorrectFile = False
            exit()
        return isCorrectFile

    # Extract from the a csv file the user criteria
    def getUserCriteria(self, horizon):
        # Create a list of dictionaries containing each criteria
        lstOfCriteria = []
        userRuleFilePath = ""
        data = []
        for file in self.userRulesFilename:
            if horizon in file:
                self.checkValidityOfCriteriaFile(file + ".csv")
                userRuleFilePath = "User_Rules_Files/" + file + ".csv"
        try:
            with open(userRuleFilePath) as criteria_file:
                csv_reader = csv.reader(criteria_file, delimiter=',')
                for index, row in enumerate(csv_reader):
                    data.append(row)
                    if index == 0:
                        self.edited = int(row[4])
                        data[index][4] = 0
                    # Skip the first 2 lines of the csv file since it contains header and titles
                    if index >= 2:
                        # Create a dictionary, convention of csv file allows us to hardcode this part
                        copy = {"Priority": row[0], "Type": row[1], "Sign": row[2], "Value": row[3]}
                        lstOfCriteria.append(copy)
                self.criteria = lstOfCriteria
        except:
            print("Error opening file. Either file does not exist in directory or unexpected error occurred.")
        self.changeEditedVar(userRuleFilePath, data)

    # If User Criteria file was updated we ensure to create a new saved data file
    def changeEditedVar(self, filename, data):
        try:
            with open(filename, 'w', newline='') as criteria_file:
                csv_writer = csv.writer(criteria_file, delimiter=',')
                for row in data:
                    csv_writer.writerow(row)
        except:
            print("Error opening file. Either file does not exist in directory or unexpected error occured.")

    # Using the client criteria to create a new df with only stocks that meet the criteria
    def filterAccordingToCriterion(self, forecastDF, priority):
        # Counters for keep track of amount of short and long positions
        countRed = 0
        countGreen = 0

        # Adding a weight col for later use
        if not 'weight' in forecastDF:
            forecastDF['weight'] = 0
        # Manipulating a copy
        copy = forecastDF.copy()

        # Iterate through the forecast and apply the filtering rules
        for rowNum in copy.index:
            # Count cell colours for later implementation
            colour = copy['Cell colour'][rowNum]
            if colour == "Dark Green" or colour == "Light Green":
                countGreen += 1
            else:
                countRed += 1
            try:
                cellIntable = float(copy[self.criteria[priority - 5]["Type"]][rowNum])
                value = float(self.criteria[priority - 5]["Value"])
                # Apply the rule and if it meets the criteria remove it
                if eval("cellIntable" + self.criteria[priority - 5]["Sign"] + "value"):
                    copy.drop(rowNum, inplace=True)

            except:
                print("Error applying the criteria.")
        # if countGreen < countRed:
        #     print("We haven't met your criteria. There are more red cells than green cells..")
        #     return
        # Sort filtered forecast according to predictability and then signal
        copy.sort_values(by=['Predictability', 'Signal'], inplace=True, ascending=[False, True])
        # Reset the indicies
        sortedCopy = copy.reset_index(drop=True)
        self.filteredForecast = sortedCopy
        self.priorityLevel = priority
        # If we have no stocks meeting our "high level" filtering recursively retry with a lower level
        if self.filteredForecast.empty:
            if priority - 1 > 0:
                self.filterAccordingToCriterion(forecastDF, priority - 1)
        return sortedCopy

    # Calculating rank-based weights and adding it to dataframe
    def calcWeightsForFilteredDataframe(self, filteredPortfolio):
        # Size of the filtered dataframe
        sizeOfDf = len(filteredPortfolio.index)
        # Resetting the indicies of the  dataframe
        # filteredPortfolio.reset_index(inplace=True)
        sumOfRanks = 0
        sumOfWeights = 0
        # Calculate the sum of the ranks
        for i in range(1, sizeOfDf + 1):
            sumOfRanks += i
        # Give weights to each stock in dataframe
        for index in range(1, sizeOfDf + 1):
            weight = ((sizeOfDf - index + 1) / sumOfRanks)
            sumOfWeights += weight
            filteredPortfolio.loc[index - 1, 'weight'] = weight

    # Gets the open and close price for a ticker
    def calcReturnPerTicker(self, ticker, openDate, closeDate):
        # Get opening price at the given date by creating a new instance of getPricesByTicker
        tickerOpenInfo = getPricesByTicker(ticker, openDate, None, None)
        tickerOpenInfo.setValues()
        try:
            openPrice = float(tickerOpenInfo.openingPrice.replace(',', ''))
        except:
            openPrice = 0
        del (tickerOpenInfo)
        # Get closing price at the given date by creating a new instance of getPricesByTicker
        tickerCloseInfo = getPricesByTicker(ticker, closeDate, None, None)
        tickerCloseInfo.setValues()
        try:
            closePrice = float(tickerCloseInfo.closingPrice.replace(',', ''))
        except:
            closePrice = 0
        del (tickerCloseInfo)
        # Return open and close price
        return openPrice, closePrice

    # Calculating the return for each stock in the filtered dataframe
    def addReturnCol(self, portfolio, filteredPortfolio, horizon):
        # Get closing date and convert it to correct format (closing date = opening+horizon)
        getInfoFromForecast.setClosingDate(self, horizon)
        getInfoFromForecast.convertClosingDate(self)
        # For each ticker calculate the return
        sizeOfDf = len(filteredPortfolio.index)
        for index in range(1, sizeOfDf + 1):
            ticker = filteredPortfolio.loc[index - 1, 'Name']
            try:

                preProcess = self.checkIfValueExists(self.prevDataFile, ticker, "return")

                if preProcess != -1 and not self.edited:
                    rateOfReturn = float(preProcess)
                else:
                    openDate = self.openingdate
                    print(openDate)
                    closeDate = self.closingDate
                    openPrice, closePrice = self.calcReturnPerTicker(ticker, openDate, closeDate)
                    rateOfReturn = ((closePrice - openPrice) / openPrice) * 100
            except:
                print("Error calculating rate of return for: ", ticker)
                rateOfReturn = 0
            print("Calculating for :", ticker)

            # Add the return to the dataframe
            filteredPortfolio.loc[index - 1, 'return'] = rateOfReturn

    # Calculating returns for the entire portfolio sum of each ticker is weight*return
    def calcRateOfReturnForPortfolio(self, portfolio):
        returnForPortfolio = 0.0
        sizeOfDf = len(self.filteredForecast.index)
        for index in range(1, sizeOfDf + 1):
            weightOfTicker = portfolio.loc[index - 1, 'weight']
            returnOfTicker = portfolio.loc[index - 1, 'return']
            returnForPortfolio += (weightOfTicker * returnOfTicker)
        self.portfolioReturn = returnForPortfolio
        return returnForPortfolio

    # Calculates the rate of return for each forecast and returns a list of dictionaries in the form of Horizon:RR
    def calcRateOfReturnForAllHorizons(self):
        lstOfReturns = []
        for horizondf in self.lstOfHorizonDf:
            for horizon in horizondf:
                print("\nCalculating RR for: ", horizon, "\n")
                self.getUserCriteria(horizon)
                print(self.criteria)
                self.forecastDataframeHorizon = horizondf[horizon]
                self.filteredForecast = self.filterAccordingToCriterion(self.forecastDataframeHorizon, 5)
                self.calcWeightsForFilteredDataframe(self.filteredForecast)
                self.addReturnCol(horizon, self.filteredForecast, horizon)
                rateOfReturn = self.calcRateOfReturnForPortfolio(self.filteredForecast)
                print(self.filteredForecast)
                print("\n")
                dictForHorizon = {"Horizon": horizon, "Return": rateOfReturn}
                lstOfReturns.append(dictForHorizon)
        self.lstOfReturnsPerHorizon = lstOfReturns
        return lstOfReturns

    # Check previously calculated data for a fields value by ticker name
    def checkIfValueExists(self, filename, ticker, field):
        try:
            with open(filename) as prev_calc_data_file:
                csv_reader = csv.reader(prev_calc_data_file, delimiter=',')
                value = -1
                # Iterating through the file row by row
                for index, row in enumerate(csv_reader):
                    # Find what column number the field is in
                    if index == 0:
                        for j in range(len(row)):
                            if row[j] == field:
                                colNum = j
                    if row[1] == ticker:
                        value = row[colNum]
            return value
        except:
            # print("Error opening file. Either file does not exist in directory or unexpected error occured.")
            return -1

    # Export all information to csv to avoid recalculating next time
    def extractToStorageFile(self):
        # Empty dict is for adding a blank line to the file without having to use xlsx methods
        emptyDict = {"Name": "", "Horizon": "", "Predictability": "", "Signal": "", "Frequency": "", "Cell colour": "",
                     "weight": "",
                     "return": ""}
        dict = {"Name": "", "Horizon": "", "Predictability": "", "Signal": "", "Frequency": "", "Cell colour": "",
                "weight": "Total RR",
                "return": self.portfolioReturn}
        dataForPrev = self.filteredForecast.append(emptyDict, ignore_index=True)
        dataForPrev = self.filteredForecast.append(dict, ignore_index=True)
        # Exporting the info to the file
        dataForPrev.to_csv(self.prevDataFile)

    # Extract from the a csv file the user strategy
    def parseStrategyFile(self, horizon):
        # Create a list of dictionaries containing each criteria
        lstOfStrategies = []
        # Using the file name to create a relative path of the file location
        strategyFilePath = "User_Rules_Files/" + self.strategyFile + ".csv"
        try:
            with open(strategyFilePath) as strategy_File:
                csv_reader = csv.reader(strategy_File, delimiter=',')
                for index, row in enumerate(csv_reader):
                    # Skip the title row
                    if index >= 2:
                        # Create a dictionary out of a row
                        copy = {"Parameter": row[0], "Condition": row[1], "Value": row[2], "Logic": row[3],
                                "Priority": row[4], "Action": row[4]}
                        lstOfStrategies.append(copy)
        except:
            print("Error opening file. Either file does not exist in directory or unexpected error occurred.")
        self.strategy = lstOfStrategies

    # Adding the beta column, which will be utilized when applying strategy
    def addBetaColumn(self, portfolio, date):
        # Adding the a col for Beta
        portfolio['Beta'] = 0.0
        for rowNum in portfolio.index:
            tickerName = portfolio['Name'][rowNum]
            # Check if we have the value pre-calculated
            preProcessBeta = self.checkIfValueExists(self.prevDataFile, tickerName, "return")
            # If we found the value in the pre-calculated information use it
            if preProcessBeta != -1:
                portfolio['Beta'][rowNum] = preProcessBeta
            else:
                # Create an instance of the data processing class
                priceObj = getPricesByTicker(tickerName, date)
                # Get the beta from Yahoo
                priceObj.getBetaValue()
                betaValue = priceObj.betaValue
                portfolio['Beta'][rowNum] = betaValue

    # Applying the strategy to the filtered forecast and return a new forecast
    def applyStrategy(self, filterPortfolio, strategies):
        appliedStrategy = False
        afterStartegyPortfolio = filterPortfolio
        for strategy in strategies:
            # if not appliedStrategy or strategy['Logic'] == "OR":
            if strategy['Condition'] == "=":
                afterStartegyPortfolio = filterPortfolio.head(int(strategy['Value']))
                print(afterStartegyPortfolio)
                # appliedStrategy = True
            else:
                for rowNum in afterStartegyPortfolio.index:
                    try:
                        cellValue = str(afterStartegyPortfolio[strategy["Parameter"]][rowNum])
                        condition = str(strategy["Condition"])
                        value = str(strategy["Value"])

                        # Apply the rule and if it meets the criteria remove it
                        if eval(cellValue + condition + value):
                            print(afterStartegyPortfolio)

                        else:
                            afterStartegyPortfolio.drop(rowNum, inplace=True)
                    except:
                        print("Error applying the strategy.")
        print(afterStartegyPortfolio)

    # Converts a date in the format of DD MM YYYY to MM/DD/YYYY
    def convertDateToUS(self, date):
        date = date.replace(" ", "")
        return date[2:4] + "/" + date[:2] + "/" + date[4:]

    # Prepares the data from the black box
    def prepareDataInputForBB(self):
        inputData = []
        # Every entry in the csv file must be of this format
        inputRow = {"forecast_date": "", "ticker": "", "pred_14d": 0, "pred_1m": 0, "pred_1y": 0,
                    "pred_3d": 0, "pred_3m": 0, "pred_7d": 0, "previous_close": 0, "signal_14d": 0, "signal_1m": 0,
                    "signal_1y": 0, "signal_3d": 0, "signal_3m": 0, "signal_7d": 0}

        lst = []
        # Add all the forecasts to a list in order to concatenate them into one big dataframe
        for horizondf in self.lstOfHorizonDf:
            for horizon in horizondf:
                lst.append(horizondf[horizon])
        wholeForecast = pd.concat(lst, ignore_index=True, sort=False)
        print(wholeForecast)

        # Iterrate through the entire forecast selecting each ticker
        for index in wholeForecast.index:
            ticker = wholeForecast["Name"][index]
            copy = inputRow.copy()
            # Setting date information for the getPrice class
            getInfoFromForecast.setClosingDate(self, horizon)
            getInfoFromForecast.convertClosingDate(self)
            openDate = self.openingdate
            closeDate = self.closingDate
            dateUS = self.convertDateToUS(openDate)
            copy["ticker"] = ticker
            copy["forecast_date"] = dateUS
            # Get the price info
            done = False
            # For a single ticker we will look through the entire dataframe to get all info for each horizon
            for i in wholeForecast.index:
                # We find another occurance of the ticker in the dataframe
                if wholeForecast["Name"][i] == ticker:
                    horizon = wholeForecast["Horizon"][i]
                    # Only calculate the previous close price once so we control it by a boolean
                    if not done:
                        prev = self.checkIfValueExists("User_Rules_Files/Prev_calc_Input_For_BlackBox.csv", ticker,
                                                       "previous_close")
                        # If we successfully found the information in prev_calc_file use it
                        if prev != -1:
                            copy["previous_close"] = prev
                        # Scrape from Yahoo
                        else:
                            openPrice, closePrice = self.calcReturnPerTicker(ticker, openDate, closeDate)
                            copy["previous_close"] = openPrice
                        done = True
                    # Depending on the horizon set the information to the correct places
                    if wholeForecast["Horizon"][i] == "3d":
                        copy["pred_3d"] = wholeForecast["Predictability"][i]
                        copy["signal_3d"] = wholeForecast["Signal"][i]
                    elif horizon == "14d":
                        copy["pred_14d"] = wholeForecast["Predictability"][i]
                        copy["signal_14d"] = wholeForecast["Signal"][i]
                    elif horizon == "7d":
                        copy["pred_7d"] = wholeForecast["Predictability"][i]
                        copy["signal_7d"] = wholeForecast["Signal"][i]
                    elif horizon == "30d":
                        copy["pred_1m"] = wholeForecast["Predictability"][i]
                        copy["signal_1m"] = wholeForecast["Signal"][i]
                    elif horizon == "90d":
                        copy["pred_3m"] = wholeForecast["Predictability"][i]
                        copy["signal_3m"] = wholeForecast["Signal"][i]
                    elif horizon == "year":
                        copy["pred_1y"] = wholeForecast["Predictability"][i]
                        copy["signal_1y"] = wholeForecast["Signal"][i]
            # Add to our list of dicts the new dictionary
            inputData.append(copy)
            done = False
        # Convert our list od dictionaries to a dataframe for easier conversion to csv
        dataForBB = pd.DataFrame(inputData)
        print(dataForBB)
        datafilecsv = "User_Rules_Files/Input_For_BlackBox.csv"
        editPrevCalcInfo = "User_Rules_Files/Prev_calc_Input_For_BlackBox.csv"
        # Store data in csv files
        dataForBB.to_csv(datafilecsv, index=False)
        dataForBB.to_csv(editPrevCalcInfo, index=False)


# TEST
#test = userBasedRules(["User_Rules_Files/IKForecast_Israel_flat_01_Sep_2019.csv"], "01/09/2019", "3d", "Israel",
#                      ["User_Filter_Criteria_3d", "User_Filter_Criteria_7d", "User_Filter_Criteria_14d",
#                       "User_Filter_Criteria_30d", "User_Filter_Criteria_90d", "User_Filter_Criteria_year"],
#                      3, "User_Strategy_Criteria")
#test.setValues()
# test.parseStrategyFile(test.horizon)
# for i in test.strategy:
#     print(i)
# test.applyStrategy(test.filteredForecast, test.strategy)
#test.prepareDataInputForBB()


