# -*- coding: utf-8 -*-
"""
@author: Karthic Krishnan


"""


#load Libraries
import pandas as pd
import numpy as np
from elasticsearch import helpers, Elasticsearch
import datetime
from datetime import date
import os
from os import listdir
from os.path import isfile, join
import warnings
warnings.filterwarnings("ignore")


os.chdir('C:\KK DRIVE\Project_GTS_ANALYTICS\DATA\Schneider Electric\ECM')
inputFiles = [f for f in listdir('.') if isfile(f)]

data_all = pd.DataFrame()
for file in inputFiles:

    infile = file
    outfile = "cleaned_files/"+infile
    delete_list = ['"', ',']
    try:
        with open(infile) as fin, open(outfile, "w+") as fout:
            for line in fin:
                for word in delete_list:
                    line = line.replace(word, "")
                fout.write(line)
        print(outfile)
        date_for_file = file.split('_')[-1].split('.')[0]
        date_for_file = datetime.datetime.strptime(date_for_file, "%y%m%d").date()
        df = pd.read_csv(outfile, sep=';', skiprows=2)
        df['Extrcted_Date'] = date_for_file 
        df['device_vulnerable'] = int(df[(df['HC Required'] == 'y')  & (df['Off HC Status'] == 'valid') & (df['Findings'] > 0)]['HC Required'].count() + df[(df['Off HC Status'] == 'expired') | (df['Off HC Status'] == 'missing') ]['Off HC Status'].count())

    except:
        try:
            df = pd.read_excel(infile)
        except:
            print('ERROR')
            print(infile)
            continue
    print('Number of records :',str(df.shape[0]))
    data_all = pd.concat([data_all,df], axis=0)
    data_all = data_all.loc[:, ~data_all.columns.str.contains('^Unnamed')]

data = data_all.copy()


# Need to be updated with Customer Name field in data
data['Customer']='Schneider Electric'

#Extracted date from file name
data['Extrcted_Date']=pd.to_datetime(data['Extrcted_Date'], errors='coerce')



#################SESDR Data Integration##################
#load SESDR data(recent dated sesdr data file- data mentioned as suffix in data filename)
sesdr = pd.read_excel('C:\KK DRIVE\Project_GTS_ANALYTICS\DATA\Schneider Electric\SESDR\SESDR-FR5SE1 - SCHNEIDER ELECTRIC IOM-CPU 20220607.xlsb')
sesdr.SCP_SERVER_TYPE.replace(' ',"Not Available")
sesdr.rename(columns = {'IP_ADDRESS':'IP address'},inplace =True)
data = data.merge(sesdr[['SCP_SERVER_TYPE','IP address']],on = 'IP address',how = 'left')

#################Data Preparation part####################
df1=data[data['HC Required']=='y']
df2=df1.groupby(['Customer','Category'], as_index=False).agg({'Off HC Status':'count'})

#Metric calculation for OP3%
df2=df2.rename(index=str, columns={'Off HC Status':'OP1'})
data_new=data.merge(df2,on=['Customer','Category'],how='left')
df3=df1[df1['Off HC Status']=='valid']
df4=df3.groupby(['Customer','Category','Off HC Status'], as_index=False).agg({'System_ID':'count'})
df4=df4.rename(index=str, columns={'System_ID':'OP2'})
data_final=data_new.merge(df4,on=['Customer','Category','Off HC Status'],how='left')
data_final['OP3%']=((data_final['OP2']/data_final['OP1'])*100).round(2)
df1=data_final

#Formatting Date fields
df1['First Prod Date'] = pd.to_datetime(df1['First Prod Date'], errors='coerce')
df1['First Prod Date']=df1['First Prod Date'].where(df1['First Prod Date'].notnull(), "1900-01-01")
df1['Off Expiry date'] = pd.to_datetime(df1['Off Expiry date'], errors='coerce')
df1['Off Expiry date']=df1['Off Expiry date'].where(df1['Off Expiry date'].notnull(), "1900-01-01")
df1['Off Last Scan'] = pd.to_datetime(df1['Off Last Scan'], errors='coerce')
df1['Off Last Scan']=df1['Off Last Scan'].where(df1['Off Last Scan'].notnull(), "1900-01-01")
df1['Last Scan'] = pd.to_datetime(df1['Last Scan'], errors='coerce')
df1['Last Scan']=df1['Last Scan'].where(df1['Last Scan'].notnull(), "1900-01-01")
df1['Last Scan'] = pd.to_datetime(df1['Last Scan'], errors='coerce')
df1['Last Scan']=df1['Last Scan'].where(df1['Last Scan'].notnull(), "1900-01-01")
df1['Off Findings'] = pd.to_datetime(df1['Off Findings'], errors='coerce')
df1['Off Findings']=df1['Off Findings'].where(df1['Off Findings'].notnull(), "1900-01-01")
df1['Off Next Expected Event Date'] = pd.to_datetime(df1['Off Next Expected Event Date'], errors='coerce')
df1['Off Next Expected Event Date']=df1['Off Next Expected Event Date'].where(df1['Off Next Expected Event Date'].notnull(), "1900-01-01")
df1['Off Expiry date'] = pd.to_datetime(df1['Off Expiry date'], errors='coerce')
df1['Off Expiry date']=df1['Off Expiry date'].where(df1['Off Expiry date'].notnull(), "1900-01-01")
df1['Off Next to expiry date'] = pd.to_datetime(df1['Off Next to expiry date'], errors='coerce')
df1['Off Next to expiry date']=df1['Off Next to expiry date'].where(df1['Off Next to expiry date'].notnull(), "1900-01-01")
df1['HC Except Date'] = pd.to_datetime(df1['HC Except Date'], errors='coerce')
df1['HC Except Date']=df1['HC Except Date'].where(df1['HC Except Date'].notnull(), "1900-01-01")
df1['HC Except Date'] = pd.to_datetime(df1['HC Except Date'], errors='coerce')
df1['HC Except Date']=df1['HC Except Date'].where(df1['HC Except Date'].notnull(), "1900-01-01")
df1=df1.drop(columns=['HC Except Reason','HC Except Text', 'Owner E-Mail'])
df1=df1.fillna(0)

#Naming convention 
df1.rename(columns = {'SCP_SERVER_TYPE':'operational_status','IP address':'host_name'},inplace=True)
df1.operational_status.replace("PRODUCTION","Production",inplace=True)
df1['IMT'] = 'IMT France'
df1['company'] = 'Schneider Electric'


#Compliance metrics
df1['Control Compliance'] = round((len(df1[df1['Off HC Status'] == "valid"])/len(df1[df1['HC Required'] == "y"]))*100,2) 
df1['Findings Compliance'] =  round( ((sum(df1['Check Number Without Info']) - (sum(df1['Check Number of Red & Yellow Findings']))) / sum(df1['Check Number Without Info']))*100,2)
df1['SAT-UNSAT_Findings%'] = np.where(df1['Findings Compliance'] >= 98,"SAT","UNSAT")
df1['Avg Findings'] = round(sum(df1['Check Number of Red & Yellow Findings']) / len(df1[df1['Off HC Status'] == "valid"]),2)
print(df1.shape)

#Create main pillar for device category
df1['device_category'] = df1.Category
df1.device_category.replace(["HP-UX","Linux","UnixWare"],"UNIX/LINUX",inplace=True)
df1.device_category.replace("ORACLE TSAM PLUS 12.1.1.1","ORACLE",inplace=True)
df1.device_category.replace(["SOLARIS","SUN"],"SUN SOLARIS",inplace=True)
df1.device_category.replace(["Windows 2000","Windows 2003","Windows 2008","Windows 2012","Windows 2016","Windows 2019"],"Windows",inplace=True)


#drop where no extraction date
df1 = df1[df1.Extrcted_Date != 0]

#Verify and observe data before ELK data ingestion
#df1 = df1.head(10)
#observe data
#df1.head(1).T
#df1.isnull().sum()




#####################Data ingestion part#############################
#converting data frame to document, Index creation and data ingestion
documents = df1.to_dict(orient='records')
'''
es = Elasticsearch(host="158.87.122.60", port=9200,scheme="https", http_auth=('deansra','d}3@k%sbGwcJ=bcC'),verify_certs=False,timeout=600)
es.indices.delete(index='dean_sra-dev_wpp-os_current_new', ignore=[400, 404])
es.indices.create(index='dean_sra-dev_wpp-os_current_new',body={},ignore=[400, 404])
helpers.bulk(es, documents, index='dean_sra-dev_wpp-os_current_new', doc_type='docs',  ignore=400)
print ("done")
'''

#index creation sample
es = Elasticsearch(host="158.87.122.60", port=9200,scheme="https", http_auth=('deansra','d}3@k%sbGwcJ=bcC'),verify_certs=False,timeout=600)
es.indices.delete(index='dean_sra-dev_ecm_sample', ignore=[400, 404])
es.indices.create(index='dean_sra-dev_ecm_sample',body={},ignore=[400, 404])
helpers.bulk(es, documents, index='dean_sra-dev_ecm_sample', doc_type='docs',  ignore=400)
print ("done")
