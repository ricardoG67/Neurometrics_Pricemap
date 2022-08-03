import time
import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup

import pricemap

import plotly.graph_objects as go
import numpy as np
import chart_studio
import chart_studio.plotly as py

from apscheduler.schedulers.blocking import BlockingScheduler

user_name = "ricardo.delacruz"
api_key = "20Ilxq1trhOcXgRcwYdZ"
chart_studio.tools.set_credentials_file(username=user_name, api_key=api_key)


def create_fig(retailers, skus, title, price_evolution_data):   
    ## create traces
    fig = go.Figure()

    ## navigate for each retail and sku
    for retail, sku in zip(retailers, skus):
        ## select the sku and retail
        query = price_evolution_data.loc[(price_evolution_data['sku'] == sku) & (price_evolution_data['retail'] == retail)]
        price = query["price_float"] ## get the price
        date = query["date_time"] ## get the date

        fig.add_trace(go.Scatter(x=date.values,
                                 y=price.values,                                
                                 name=retail))
        
    fig.update_traces(mode='lines+markers')
    fig.update_layout(title={
                            'text': title,
                            'y':0.9,
                            'x':0.5,
                            'xanchor': 'center',
                            'yanchor': 'top'})
    
    return fig

sched = BlockingScheduler()

@sched.scheduled_job('cron', minute='03', hour='13,23')
def update_prices():
    ## init time
    start_time = time.time()
    
    ##################################
    ##### PREPARE THE DATAFRAME
    ################################### 
    price_evolution_data = pd.read_csv("price_evolution.csv")
    retail_data = pd.read_csv("retail_data.csv")

    ## create and get the values to fill csv
    df = pd.DataFrame(columns=price_evolution_data.columns)
    df["sku"], df["price"], df["retail"], df["date"], df["time"]= zip(*retail_data.apply(lambda x: pricemap.get_price_retail(x["uri"], x["retail"], x["sku"]), axis=1))
    
    ## drop missing values
    #df.dropna(how="any", inplace=True)

    ## concat pandas and append raws axis=0
    price_evolution_data = pd.concat([price_evolution_data, df], axis=0, ignore_index=True)
    price_evolution_data.dropna(how="any", inplace=True)
    price_evolution_data.to_csv("price_evolution.csv", index=False)
    
    ## get the size
    size = price_evolution_data.shape
    
    ## greater than the start date 
    seven_days_ago = pricemap.get_time(today=False)
    price_evolution_data = price_evolution_data.loc[price_evolution_data['date'] >= seven_days_ago[0]]    

    ## transform datatime
    price_evolution_data["price_float"] = price_evolution_data["price"].apply(lambda x: float(x.split(" ")[1].replace(',','')))
    price_evolution_data["date_time"] = price_evolution_data[["date", "time"]].apply(lambda row: " ".join(row.values), axis=1)

    ## Inka Cola 500ml
    retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
    skus = ["59539001", "59539001", "497497", "inca-kola-gaseosa-10174358/p/", "497497"]
    title = 'INKA COLA 500ML'
    fig = create_fig(retailers, skus, title, price_evolution_data)
    py.plot(fig, filename="inka_cola_500ml", auto_open=False)

    print("Update executed: ", pricemap.get_time())
    print("--- %s seconds ---" % (time.time() - start_time))
    print("Size Evolution Dataframe: ", size)

sched.start()
