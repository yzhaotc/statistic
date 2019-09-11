# -*- coding: utf-8 -*-
"""
Created on Mon Jul  8 17:39:47 2019

@author: yzhao
"""

import pandas as pd
import psycopg2
import csv
import os

def conncet(dbname,user,host,password,port):
    conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s' port='%s'"%(dbname,user,host,password,port))
    return(conn)




def get_data(conn,sympol):
    datelist=[]
    conn.rollback()
    cur=conn.cursor()
    cur.execute("SELECT hour_block,exchange_symbol,all_count FROM sentiment.t200hourly_counts as sent where sent.exchange_symbol\
                like '%s'"%(sympol))
    caocao=cur.fetchall()
    name=sympol
    csvname='C:\\macroeconomic\\market\\'+name+'.csv'
    with open(csvname,'w') as out:
        csv_out=csv.writer(out)
        csv_out.writerow(['hour_block','exchange_symbol','all_count'])
        for row in caocao:
            csv_out.writerow(row)
    return(datelist)




with open('symbol_list.txt','r',encoding='utf8') as f:
    exchange_symbol=f.readlines()


conn1=conncet(dbname='sentiment',user='senti',host='db-sentiment.dev.recognia.com',password='Zse4rfV',port='6432')
for item in exchange_symbol:
    item=item.replace('\n','')
    datelist=get_data(conn1,item)


'formalization the csv file'
files=os.listdir('C:\\macroeconomic\\market\\')
for file in files:
    fullname='C:\\macroeconomic\\market\\'+file
    df=pd.read_csv(fullname)
    df['hour_block'] = pd.to_datetime(df['hour_block'])
    df = df.set_index('hour_block')
    df_hour=df.resample('H').sum()    
    df_day=df_hour.resample('D').sum()
    newpath='C:\\macroeconomic\\market_new\\'+file
    df_day.to_csv(newpath)



'save them in to one excel file'
df1=pd.read_csv(r'C:\\macroeconomic\\market_new\\NYMEX.csv')
timelist=list(df1['hour_block'])
files=os.listdir(r'C:\\macroeconomic\\market_new\\')
count_list=[]
dic={}
final_list=[]
for file in files:
    name=file.split('.')[0]
    fullname='C:\\macroeconomic\\market_new\\'+file
    df=pd.read_csv(fullname)
    for item in timelist:
        try:
            count=df[df['hour_block']==item]['all_count'].values[0]
        except:
            count=0
        count_list.append(count)
    final_list.append(count_list)
    count_list=[]

    
df_final=pd.DataFrame.from_records(final_list)
df_final=df_final.T
newname=[x.split('.')[0] for x in files]
df_final.columns=newname

df_final['time']=timelist
df_final = df_final.set_index('time')

df_final.to_csv('market.csv')
#'Analysis by market'

#df=pd.read_csv(r'C:\macroeconomic\z_score\caonima.csv')
#df['hour_block'] = pd.to_datetime(df['hour_block'])
#df = df.set_index('hour_block')
#df_day=df.resample('D').sum()
#
#exchange_symbol=list(set(list(df['exchange_symbol'])))


#with open('symbol_list.txt','w',encoding='utf8') as f:
#    for element in exchange_symbol:
#        f.write(element)
#        f.write('\n')
    