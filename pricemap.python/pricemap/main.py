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

###################################
##### CONFIGURE CREDENTIALS - PLOTLY
################################### 
user_name = "ricardo.delacruz"
api_key = "20Ilxq1trhOcXgRcwYdZ"
chart_studio.tools.set_credentials_file(username=user_name, api_key=api_key)

###################################
##### PLOT THE PRICES FROM RETAILERS
################################### 
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


##################################
##################################
##### START THE SCHEDULE
##################################
##################################
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


    ###################################
    ##### REFRESH PRICES FROM EACH PRODUCT
    ################################### 

#     ## SPRITE 500ML
#     retailers = ["wong", "metro", "plaza_vea"]
#     skus = ["13189", "13189", "382618"]
#     title = "SPRITE 500ML"
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="sprite_500ml", auto_open=False)

#     ## AJINOMOTO 500GR
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["4340", "4340", "163941", "ajinomoto-sazonador-10164368/p/", "163941"]
#     title = "AJINOMOTO 500GR"
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="ajinomoto_500gr", auto_open=False)

#     ## ACEITE PRIMOR CLÁSICO
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["3201", "3201", "161", "primor-aceite-vegetal-40896347/p/", "161"]
#     title = "ACEITE PRIMOR CLÁSICO"
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="aceite_primor_clasico", auto_open=False)

    ## Televisor LG NanoCell 49"
    retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
    skus = ["793650", "793650", "20197304", "lg-tv-nanocell-49-ultrahd-4k-thinq-ai-42127276/p/", "televisor-lg-nanocell-led-49---4k-smart-tv-ai-49nano81--2020--38203/p"]
    title = 'TELEVISOR LG NANOCELL 49"'
    fig = create_fig(retailers, skus, title, price_evolution_data)
    py.plot(fig, filename="televisor_lg_nanocell_49", auto_open=False)

    ## arroz extra costeño bolsa 5kg
    retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
    skus = ["14974", "14974", "641425", "costeno-arroz-extra-10472604/p/", "641425"]
    title = 'ARROZ EXTRA COSTEÑO BOLSA 5Kg'
    fig = create_fig(retailers, skus, title, price_evolution_data)
    py.plot(fig, filename="arroz_extra_costeno_bolsa_5kg", auto_open=False)

    ## Whisky Johnnie Walker Red Label 1l
    retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
    skus = ["727725", "727725", "216753", "johnnie-walker-whisky-red-label-con-estuche-10178071/p/", "216753"]
    title = 'WHISKY JOHNNIE WALKER RED LABEL 1L'
    fig = create_fig(retailers, skus, title, price_evolution_data)
    py.plot(fig, filename="whisky_johnnie_walker_red_label_1l", auto_open=False)
    
    ## Coca Cola 500ml
    retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
    skus = ["3814", "3814", "281027", "coca-cola-gaseosa-no-retornable-10164146/p/", "281027"]
    title = 'COCA COLA 500ML'
    fig = create_fig(retailers, skus, title, price_evolution_data)
    py.plot(fig, filename="coca_cola_500ml", auto_open=False)
    
    ## Inka Cola 500ml
    retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
    skus = ["59539001", "59539001", "497497", "inca-kola-gaseosa-10174358/p/", "497497"]
    title = 'INKA COLA 500ML'
    fig = create_fig(retailers, skus, title, price_evolution_data)
    py.plot(fig, filename="inka_cola_500ml", auto_open=False)
    
#     ## Pepsi 500ml
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["103181", "103181", "64718", "pepsi-gaseosa-10456593/p/", "64718"]
#     title = 'PEPSI 500ML'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="pepsi_500ml", auto_open=False)
    
#     ## Pan de molde blanco bimbo x 500gr
#     retailers = ["wong", "plaza_vea", "tottus"]
#     skus = ["141803", "20019588", "bimbo-pan-de-molde-blanco-mediano-40457334/p/"]
#     title = 'PAN DE MOLDE BLANCO BIMBO X 500GR'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="pan_de_molde_blanco_bimbo_500gr", auto_open=False)
    
#     ## Leche evaporada entera Gloria lata x 400gr
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["59521001", "59521001", "949", "gloria-leche-evaporada-entera-41119510/p/", "949"]
#     title = 'LECHE EVAPORADA ENTERA GLORIA LATA X 400GR'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="leche_evaporada_entera_gloria_lata_500gr", auto_open=False)
    
    ## Huevos pardos 15 unid. (marca propia: Wong, metro, bells plaza vea, bells vivanda, tottus)
    retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
    skus = ["723464", "3323", "2294", "tottus-huevos-pardos-40293807/p/", "2294"]
    title = 'HUEVOS PARDOS 15 UNID. (MARCA PROPIA)'
    fig = create_fig(retailers, skus, title, price_evolution_data)
    py.plot(fig, filename="huevos_pardos_15_unid_marca_propia", auto_open=False)
    
#     ## Arroz extra costeño x 750gr
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["11386", "11386", "433778", "costeno-arroz-extra-10472603/p/", "433778"]
#     title = 'ARROZ EXTRA COSTEÑO X 750GR'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="arroz_extra_costenio_750gr", auto_open=False)
    
#     ## Filete de pechuga de pollo x 1kg
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["227882", "227882", "20110126", "tottus-filete-de-pechuga-pollo-40433295/p/", "20110126"]
#     title = 'FILETE DE PECHUGA DE POLLO X 1KG'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="filete_de_pechuga_de_pollo_1kg", auto_open=False)
   
#     ## Manzana roja importada x 1kg
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["67289", "67289", "20064830", "frutas-manzana-roja-importada-premium-40632061/p/", "20064830"]
#     title = 'MANZANA ROJA IMPORTADA X 1KG'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="manza_roja_importada_1kg", auto_open=False)
    
#     ## Tomate x 1kg (Wong suelto, marca metro, plaza vea bells, vivanda bells, tottus suelto)
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["4044", "393213", "20198834", "tottus-tomate-italiano-10162958/p/", "20198834"]
#     title = 'TOMATE X 1KG (MARCA PROPIA)'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="tomate_1kg_marca_propia", auto_open=False)
    
#     ## Azúcar rubia x 1kg (marca propia: Wong, metro, bells plaza vea, bells vivanda, tottus)
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["116641", "116642", "6757", "tottus-azucar-rubia-40546552/p/", "6757"]
#     title = 'AZÚCAR RUBIA X 1KG (MARCA PROPIA)'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="azucar_rubia_1kg_marca_propia", auto_open=False)
   
#     ## Lenteja Costeño x 1kg
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["30037", "30037", "995413", "costeno-lenteja-10167792/p/", "995413"]
#     title = 'LENTEJA COSTEÑO X 1KG'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="lenteja_costeño_1kg", auto_open=False)
    
#     ## Spaghetti Don Vittorio x 500gr
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["11112", "11112", "1107105002", "don-vittorio-fideo-spaghetti-40747602/p/", "1107105002"]
#     title = 'SPAGHETTI DON VITTORIO X 500GR'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="spaghetti_don_victorio_500gr", auto_open=False)
    
    ## Queso edam Braedt paquete x 200g
    retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
    skus = ["752207", "752207", "20172513", "braedt-queso-edam-41848153/p/", "20172513"]
    title = 'QUESO EDAM BRAEDT PAQUETE X 200GR'
    fig = create_fig(retailers, skus, title, price_evolution_data)
    py.plot(fig, filename="queso_edam_braedt_paquete_200gr", auto_open=False)
    
#     ## Yogurt fresa Gloria x 1kg
#     retailers = ["wong", "metro", "plaza_vea", "tottus"]
#     skus = ["48255001", "48255001", "20192547", "gloria-yogurt-fresa-gloria-x-1kg-42084737/p/"]
#     title = 'YOGURT FRESA GLORIA X 1KG'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="yogurt_fresa_gloria_1kg", auto_open=False)
    
    ## Mantequilla con sal Laive pote x 200gr
    retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
    skus = ["466522", "466522", "20076655", "laive-mantequilla-con-sal-41255560/p/", "20076655"]
    title = 'MANTEQUILLA CON SAL LAIVE POTE 200GR'
    fig = create_fig(retailers, skus, title, price_evolution_data)
    py.plot(fig, filename="mantequilla_laive_pote_200gr", auto_open=False)
    
    ## Impresora multifuncional HP 315
    retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
    skus = ["734634", "734634", "20148667", "impresora-multifuncional-hp-ink-tank-315-39227/p", "hp-impresora-ink-tank-315-41633098/p/"]
    title = 'IMPRESORA MULTIFUNCIONAL HP 315'
    fig = create_fig(retailers, skus, title, price_evolution_data)
    py.plot(fig, filename="impresora_multifuncional_hp_315", auto_open=False)
    
#     ## Panetón Donofrio caja 900g
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["5366", "5366", "48033", "donofrio-paneton-41198224/p/", "48033"]
#     title = 'PANETÓN DONOFRIO CAJA 900G'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="paneton_donofrio_caja_900g", auto_open=False)
    
#     ## Panetón Todinno caja 900g
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["15171", "15171", "20227", "todinno-paneton--paneton-x-100-g-10205606/p/", "20227"]
#     title = 'PANETÓN TODINNO CAJA 900G'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="paneton_todinno_caja_900g", auto_open=False)
    
#     ## Panetón Donofrio tradicional bolsa 900g
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["361514", "361514", "926322", "donofrio-paneton-40286549/p/", "926322"]
#     title = 'PANETÓN DONOFRIO TRADICIONAL BOLSA 900G'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="paneton_donofrio_tradicional_bolsa_900g", auto_open=False)

#     ## Panetón Gloria bolsa 900g
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["253275", "253275", "969673", "gloria-paneton-con-pasas-y-frutas-41742592/p/", "969673"]
#     title = 'PANETÓN GLORIA BOLSA 900G'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="paneton_gloria_bolsa_900g", auto_open=False)
    
#     ## Tableta para Taza Sabor Chocolate Clavo y Canela Sol del Cusco Tableta 90 g
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["48485002", "48485002", "20100882", "sol-del-cusco-tableta-para-taza-con-canela-41051410/p/", "20100882"]
#     title = 'TAB. CHOC. CLAV. Y CAN. SOL DEL CUSCO 90G'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="tableta_sol_del_cusco_clavo_canela_90g", auto_open=False)
    
#     ## Tableta para Taza Sabor Chocolate Tradicional Sol del Cusco Tableta 90 gr
#     retailers = ["wong", "metro", "plaza_vea", "tottus", "vivanda"]
#     skus = ["48485001", "48485001", "20100883", "sol-del-cusco-tableta-para-taza-tradicional-41051409/p/", "20100883"]
#     title = 'TAB. CHOC. TRAD. SOL DEL CUSCO 90G'
#     fig = create_fig(retailers, skus, title, price_evolution_data)
#     py.plot(fig, filename="tableta_sol_del_cusco_chocolate_tradicional_90g", auto_open=False)
    
    print("Update executed: ", pricemap.get_time())
    print("--- %s seconds ---" % (time.time() - start_time))
    print("Size Evolution Dataframe: ", size)
    
    
## run schedule
#update_prices()
sched.start()

