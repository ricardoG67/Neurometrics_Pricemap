import datetime
from pytz import timezone
import requests
import pandas as pd
from bs4 import BeautifulSoup

###################################
##### GET CURRENTLY TIME
###################################
def get_time(today = True):    
    ## now = datetime.datetime.now()
    est = timezone('EST')
        
    ## select date
    if today == True:
        now = datetime.datetime.now(est)
    
    ## 7 days ago
    else:
        now = datetime.datetime.now(est) - datetime.timedelta(days=6)
        
    year = '{:02d}'.format(now.year)
    month = '{:02d}'.format(now.month)
    day = '{:02d}'.format(now.day)
    hour = '{:02d}'.format(now.hour)
    minute = '{:02d}'.format(now.minute)
    second = "{:02d}".format(now.second)

    ## join the values 
    current_date = '{}-{}-{}'.format(year, month, day)
    current_time = "{}:{}:{}".format(hour, minute, second)

    return current_date, current_time

###################################
##### GET PRICES FROM RETAILERS
###################################    
def get_price_retail(uri, retail, sku):
    
    ###################################
    ##### WONG
    ###################################
    if retail == "wong":
        try:
            ## define the URI
            product_uri = uri

            #get the content of the image
            page = requests.get(product_uri).content

            ## get the soup
            soup = BeautifulSoup(page,'lxml') ##html.parser

            ## get the price of the product
            price = soup.find('strong', {"class":"skuBestPrice"}).text
        
        except:      
            price = None 

    ###################################
    ##### PLAZA VEA
    ###################################
    if retail == "plaza_vea":
        try:            
            ## define the URI
            product_uri = uri

            #get the content of the image
            page = requests.get(product_uri).content

            ## get the soup
            soup = BeautifulSoup(page,'lxml') ##html.parser

            ## get the price of the product
            sku = soup.find("span", {"class":"ProductCard__sku"}).find("div").text

            ## define URl by SKU
            search_url = "https://www.plazavea.com.pe/Busca/?PS=20&cc=24&sm=0&PageNumber=1&O=OrderByScoreDESC&fq=alternateIds_RefId%3A" + sku

            #get the content of the image
            page = requests.get(search_url).content

            ## get the soup
            soup = BeautifulSoup(page,'lxml') ##html.parser

            ## get the price of the product
            price = soup.find("div", {"class":"Showcase__salePrice"}).text
        
        except:      
            price = None 

    ###################################
    ##### METRO
    ###################################
    if retail == "metro":
        try:            
            ## define the URI
            product_uri = uri

            #get the content of the image
            page = requests.get(product_uri).content

            ## get the soup
            soup = BeautifulSoup(page,'lxml') ##html.parser

            ## get the price of the product
            price = soup.find('strong', {"class":"skuBestPrice"}).text
            
        except:      
            price = None 


    ###################################
    ##### TOTTUS
    ###################################
    if retail == "tottus":
        try:            
            #get the content of the url
            page = requests.get(uri).content

            ## get the soup
            soup = BeautifulSoup(page,'lxml') ##html.parser

            ## get price
            #price = soup.find('div', {"class":"jsx-1599995895 ProductPrice big"}).text.split("UN")[0].strip()
            price = soup.find('div', {"class":"jsx-2685088125 ProductPrice big"}).text.split("UN")[0].strip()
        
        except:      
            price = None 

    ###################################
    ##### VIVANDA
    ###################################
    if retail == "vivanda":
        try:            
            #get the content of the url
            page = requests.get(uri).content

            ## get the soup
            soup = BeautifulSoup(page,'lxml') ##html.parser  

            ## get price
            price = soup.find('strong', {"class":"skuBestPrice"}).text
    
        except:      
            price = None        

    current_date, current_time = get_time()
    
    return sku, price, retail, current_date, current_time
