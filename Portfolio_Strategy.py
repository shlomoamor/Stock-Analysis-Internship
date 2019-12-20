from Data_Processing_Classes import getPricesByTicker
from Extracting_Data_from_CSV import getInfoFromForecast
import pandas as pd

class portfolioStrategy(getInfoFromForecast,getPricesByTicker):
    def __init__(self,date, tickerName,forecastFileName, horizon, region):
        getPricesByTicker.__init__(self,tickerName, date)
        getInfoFromForecast.__init__(self,forecastFileName,date,horizon, region)
        self.filtertedForecast = pd.DataFrame()

    # Initializing all values using both inherited classes
    def setValues(self):
        getPricesByTicker.setValues(self)
        getInfoFromForecast.setValues(self)
