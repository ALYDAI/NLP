# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 11:47:26 2021

@author: 戴嘉萱
"""

import os
import numpy as np
import pandas as pd
# from finlab.data import Data
os.chdir(r'C:\Users\戴嘉萱\finlab')
import matplotlib.pyplot as plt
import datetime

import seaborn as sns 
import requests
import time
from bs4 import BeautifulSoup
import csv
#%%

'取得新聞列表'

# start,end=unit
def cnyes_list_crawler():
    
    now=datetime.datetime.now().date()#今天
    start_time=datetime.date(2014,1, 1)#起始日
    week=((now-start_time)/7).days#每7天一個週期
    delta=datetime.timedelta(days=7)
    ids = []

    for days in range(week):
        last_time=start_time
        start_time=last_time+delta
    
        # final_time=start_time-datetime.timedelta(days=1)
        print(last_time,start_time)
    
        start,end=int(time.mktime(last_time.timetuple())),int(time.mktime(start_time.timetuple()))-1

        for p in range(1,11):
            try :               
                url = f"https://api.cnyes.com/media/api/v1/newslist/category/tw_stock?limit=30&startAt={start}&endAt={end}&page={p}"
                print(url)
                res = requests.get(url)
                data = res.json()
                news_list = data["items"]["data"]
                for item in news_list:
                  ids.append(item["newsId"])
            except:
                print('少於10頁')
    return ids

#%%
  
'爬取新聞'
def cnyes_news_crawler(url):   
    
    ###url = "https://news.cnyes.com/news/id/4513215?exp=a"
    try :
        res = requests.get(url)
        soup = BeautifulSoup(res.text)
        title = soup.find("h1").text
        time = soup.find("time").text
        body_div = soup.find("div", {"itemprop":"articleBody"})
    
        content = ""
        for p in body_div.find_all("p"):
          content = content + p.text.strip().replace("<br>", "")
        keywords = soup.find("meta", {"itemprop":"keywords"})["content"]
        
        news_list[title]={'發文時間':time,'內文':content,'關鍵字':keywords}
        time.sleep(2)
    except:
        pass
     
    

#%%

tol_ids = cnyes_list_crawler()

# store=pd.DataFrame(tol_ids)
# store.to_csv('key.csv',index=False,index_label=False)

#%%
news_list={}

tol_ids =pd.read_csv('key.csv')

tol_ids=(tol_ids.iloc[:,0].tolist())


#%%
import concurrent.futures

news_list
urls = [f"https://news.cnyes.com/news/id/{nid}?exp=a" for nid in tol_ids]  


start_time = time.time()  # 開始時間
 
# 同時建立及啟用10個執行緒
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        
    executor.map(cnyes_news_crawler, urls)
 
end_time = time.time()
print(f"{end_time - start_time} 秒爬取 {len(urls)} 條文章")

#%%
news_table=pd.DataFrame(news_list).T

# news_table.to_csv('news_table.csv',encoding="utf_8_sig")
# news_table.to_pickle('news_table.pkl')
# ind_code = pd.read_pickle('news_table.pkl')


