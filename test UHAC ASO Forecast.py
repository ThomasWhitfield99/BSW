import warnings
warnings.filterwarnings('ignore')
import logging
logging.getLogger('fbprophet').setLevel(logging.WARNING)
import pandas as pd
import sqlalchemy
import time
from fbprophet import Prophet
from IPython import get_ipython
get_ipython().run_line_magic('matplotlib', 'inline')

start_time = time.time()

engine = sqlalchemy.create_engine('mssql+pyodbc://BSWQADBP01/BQA_Finance?driver=SQL+Server+Native+Client+11.0')

query = """\
EXEC Forecast_UHAC_ASO
"""
rawdata = pd.read_sql_query(query,engine)
payername = rawdata.get_value(1,"Payer")
payer_Hierarchy_1 = rawdata.get_value(1,"Payer_Hierarchy_1")
lastclaimsperiod = pd.to_datetime(max(rawdata['ds']))

rawdataMedical = rawdata.loc[rawdata['Claims_PLOS_Group']=='Medical']
dfMedical = rawdataMedical[['ds','y']]

my_modelMedical = Prophet(interval_width=0.95)  
my_modelMedical.fit(dfMedical)

future_datesMedical = my_modelMedical.make_future_dataframe(periods=12, freq='MS')

forecastMedical = my_modelMedical.predict(future_datesMedical)

originaldfMedical = dfMedical[['ds','y']]
forecastdfMedical = forecastMedical[['ds', 'yhat','yhat_lower','yhat_upper','trend','trend_lower','trend_upper','seasonal','seasonal_lower','seasonal_upper']]

originaldfMedical.ds = pd.to_datetime(originaldfMedical.ds)
forecastdfMedical.ds = pd.to_datetime(forecastdfMedical.ds)

sqlDFMedical=pd.merge(originaldfMedical,forecastdfMedical,how='outer',on='ds')
sqlDFMedical = sqlDFMedical.fillna('')

idx = 0
new_col = ''
sqlDFMedical.insert(loc=idx,column = 'Payer',value=payername)
sqlDFMedical.insert(loc=idx + 1,column = 'Payer_Hierarchy_1',value=payer_Hierarchy_1)
sqlDFMedical.insert(loc=idx + 2,column = 'Claims_PLOS_Group',value='Medical')

Dollar_Types = []
for row in sqlDFMedical['ds']:
    if row < lastclaimsperiod:
        Dollar_Types.append('Actual')
    else:
        Dollar_Types.append('Forecast')
sqlDFMedical.insert(loc=idx + 6,column = 'Dollar_Type',value = Dollar_Types)

sqlDFMedical.rename(columns={'ds':'Service_Period','y':'PMPM','yhat':'Forecast'
                     ,'yhat_lower':'Forecast_Lower','yhat_upper':'Forecast_Upper','trend':'Trend','trend_lower':'Trend_Lower'
                     ,'trend_upper':'Trend_Upper','seasonal':'Seasonal','seasonal_lower':'Seasonal_Lower'
                     ,'seasonal_upper':'Seasonal_Upper'}, inplace=True)

rawdataRx = rawdata.loc[rawdata['Claims_PLOS_Group']=='Rx']
dfRx = rawdataRx[['ds','y']]

my_modelRx = Prophet(interval_width=0.95)  
my_modelRx.fit(dfRx)

future_datesRx = my_modelRx.make_future_dataframe(periods=12, freq='MS')

forecastRx = my_modelRx.predict(future_datesRx)

originaldfRx = dfRx[['ds','y']]
forecastdfRx = forecastRx[['ds', 'yhat','yhat_lower','yhat_upper','trend','trend_lower','trend_upper','seasonal','seasonal_lower','seasonal_upper']]

originaldfRx.ds = pd.to_datetime(originaldfRx.ds)
forecastdfRx.ds = pd.to_datetime(forecastdfRx.ds)

sqlDFRx=pd.merge(originaldfRx,forecastdfRx,how='outer',on='ds')
sqlDFRx = sqlDFRx.fillna('')

idx = 0
new_col = ''
sqlDFRx.insert(loc=idx,column = 'Payer',value=payername)
sqlDFRx.insert(loc=idx + 1,column = 'Payer_Hierarchy_1',value=payer_Hierarchy_1)
sqlDFRx.insert(loc=idx + 2,column = 'Claims_PLOS_Group',value='Rx')

Dollar_Types = []
for row in sqlDFRx['ds']:
    if row < lastclaimsperiod:
        Dollar_Types.append('Actual')
    else:
        Dollar_Types.append('Forecast')
sqlDFRx.insert(loc=idx + 6,column = 'Dollar_Type',value = Dollar_Types)

sqlDFRx.rename(columns={'ds':'Service_Period','y':'PMPM','yhat':'Forecast'
                     ,'yhat_lower':'Forecast_Lower','yhat_upper':'Forecast_Upper','trend':'Trend','trend_lower':'Trend_Lower'
                     ,'trend_upper':'Trend_Upper','seasonal':'Seasonal','seasonal_lower':'Seasonal_Lower'
                     ,'seasonal_upper':'Seasonal_Upper'}, inplace=True)

sqlCombined = [sqlDFMedical,sqlDFRx]
Output = pd.concat(sqlCombined)

sqlOutput = Output.round(2)
sqlOutput.to_sql('testPythonOutput', engine, if_exists='replace',index = False)

timetorun = 'Program took', time.time() - start_time, 'to run'
print (timetorun)