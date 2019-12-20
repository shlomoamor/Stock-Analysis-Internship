from Extracting_Data_from_CSV import getInfoFromForecast
import pandas as pd
import csv

class userBasedRules(getInfoFromForecast):
    def __init__(self,forecast_csv, date, horizon,region, userRulesFilename):
        getInfoFromForecast.__init__(self, forecast_csv, date, horizon, region)
        self.userRulesFilename =userRulesFilename

    def setValues(self):
        PRED = 0.7
        parsedDate = self.convertGivenDateToFileDate()
        if getInfoFromForecast.checkValidityOfFile(self,parsedDate):
            getInfoFromForecast.createStockDictionariesForHorizon(self)
            getInfoFromForecast.convertListToDataFrame(self, self.listOfStocksHorizon)
            self.getUserCriteria()
            self.filterAccordingToCriterion(PRED)
            self.calcWeightsForFilteredDataframe()

    # Check that we received a valid file from the client
    def checkValidityOfFile(self):
        isCorrectFile = True
        filename = "User_Strategy_Criteria"+".csv"
        # Check if the file matches the information provided
        if filename != self.userRulesFilename:
            print("Error! Incorrect file given. \nGiven filename is: ", self.userRulesFilename, "\nExpected filename: ",
                  filename)
            isCorrectFile = False
        return isCorrectFile

    # Extract from the a csv file the clients criteria
    def getUserCriteria(self):
        self.criteria = [0,0,0,0]
        try:
            with open(self.userRulesFilename) as criteria_file:
                csv_reader = csv.reader(criteria_file, delimiter=',')
                for index, row in enumerate(csv_reader):
                    if index >= 2:
                        if row[1] == "1":
                            self.criteria[index-2] = 1
        except:
            print("Error opening file. File does not exist in directory")

    # Using the client criteria to create a new df with only stocks that meet the criteria
    def filterAccordingToCriterion(self, predLevel):
        # Counters for keep track of amount of short and long positions
        countRed = 0
        countGreen = 0
        # Adding a weight col for later use
        self.forecastDataframeHorizon['weight'] = 0
        # Manipulating a copy
        copy = self.forecastDataframeHorizon.copy()

        for criteria, index in enumerate(self.criteria):
            # If criteria is true then apply it and remove columns  that don't meet the criteria
            if criteria:
                for rowNum in copy.index:
                    colour = copy['Cell colour'][rowNum]
                    if colour == "Dark Green" or colour == "Light Green":
                        countGreen += 1
                    else:
                        countRed += 1
                    if index == 0:
                        if float(copy["Predictability"][rowNum]) < predLevel:
                            copy.drop(rowNum,inplace=True)
                    if index == 1:
                        if float(copy["Signal"][rowNum]) < 1:
                           copy.drop(rowNum, inplace=True)
        if countGreen < countRed and self.criteria[2]:
            print("We haven't met your criteria. There are more red cells than green cells..")
            return
            copy = copy.sort_values(by=['predictability', 'signal'], inplace=True, ascending=[False, True])
        self.filteredForecast = copy
        # If we have no stocks meeting our "high level" filtering retry with a lower level
        if self.filteredForecast.empty:
            self.filterAccordingToCriterion(0.15)

    # Calculating rank-based weights and adding it to dataframe
    def calcWeightsForFilteredDataframe(self):
        sizeOfDf = len(self.filteredForecast.index)
        self.filteredForecast.reset_index(inplace=True)
        sumOfRanks = 0
        sumOfWeights = 0
        for i in range(1, sizeOfDf+1):
            sumOfRanks +=i
        for index in range(1, sizeOfDf+1):
            weight = ((sizeOfDf-index+1)/sumOfRanks)
            sumOfWeights += weight
            self.filteredForecast.loc[index-1, 'weight'] = weight
        print(self.filteredForecast)

# testing
t = userBasedRules("IKForecast_Israel_flat_01_Sep_2019.csv", "01/09/2019", "year", "Israel","User_Strategy_Criteria.csv")
t.setValues()
