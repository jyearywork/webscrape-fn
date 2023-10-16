import logging
import azure.functions as func
from datetime import datetime
import pandas as pd
from azure.functions.decorators.core import DataType
from localfiles.apartments_write_data_2 import ApartmentsParser

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
write_date = datetime.now().strftime("%d-%B-%Y")

@app.function_name(name="ApartmentsComScraper")
@app.schedule(schedule="0 0 0 * * *", arg_name="myTimer",
              run_on_startup=True, use_monitor=False)
@app.blob_input(arg_name="inputblob",
                path="webscrape/apt_comps.csv",
                connection="AzureWebJobsStorage")
@app.blob_output(arg_name="outputblob", 
                 path=f"webscrape/{write_date}/{write_date}_output.csv", 
                 connection="AzureWebJobsStorage")
@app.generic_output_binding(arg_name='writeToDB', type='sql',
                            CommandText="[dbo].[apt_comps]",
                            ConnectionStringSetting='SqlConnectionString',
                            data_type=DataType.STRING)

def write_data(myTimer: func.TimerRequest,
               inputblob: func.InputStream,
               outputblob: func.Out[str], writeToDB: func.Out[func.SqlRowList]):
    
    if myTimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function executed.')

    ApartmentsParser(inputblob, "data/ApartmentscomDatabase.db", 1, mode='write')
    outputblob.set('apt_comps_output.csv')
    df=pd.read_csv('data/apt_comps_output.csv')

    split_col =  df['Number of Units and Stories'] 

    units_list = []
    stories_list = []

    n=len(split_col)
    for i in range(0,n):
        val=split_col[i]
        vals=[x.split(' ') for x in val.split('/')]
        units=vals[0][0]
        stories=vals[1][0]
        units_list = units_list + [units] # type: ignore
        stories_list = stories_list + [stories] # type: ignore
    
    df.drop('Number of Units and Stories', axis=1, inplace=True)
    df['number_of_units']=list(map(int, units_list))
    df['number_of_stories']=list(map(int, stories_list))
    output = df.to_dict('records')
    rows = func.SqlRowList(map(lambda r: func.SqlRow.from_dict(r), output))
    return writeToDB.set(rows)