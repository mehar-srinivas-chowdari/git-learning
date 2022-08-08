#from elasticsearch import Elasticsearch
#import json, requests
import pandas as pd

#es =Elasticsearch(['https://localhost:9200/'], basic_auth=('elastic', 'dmb6=xBEpXvFaMVRV4yU'))

df = pd.read_csv(r"G:\Downloads\oc_run_level_status.csv")

#selecting only required fileds

df = df[['agency_name','ids_with_errors','ids_without_errors','form_name']]

#converting dataframe into dictonary of list

lis = df.to_dict('list')

data={}

#creating data dictionary for final data

for i in range(len(lis['agency_name'])):
    if(lis['agency_name'][i] not in data):
        data[lis['agency_name'][i]]={}
        data[lis['agency_name'][i]][lis['form_name'][i]]={}
        if(str(lis['ids_without_errors'][i])!='nan'):
            data[lis['agency_name'][i]][lis['form_name'][i]][lis['ids_without_errors'][i]]=True
        if(str(lis['ids_with_errors'][i])!='nan'):
            data[lis['agency_name'][i]][lis['form_name'][i]][lis['ids_with_errors'][i]]=False
    elif(lis['form_name'][i] not in data[lis['agency_name'][i]]):
        data[lis['agency_name'][i]][lis['form_name'][i]]={}
        if(str(lis['ids_without_errors'][i])!='nan'):
            data[lis['agency_name'][i]][lis['form_name'][i]][lis['ids_without_errors'][i]]=True
        if(str(lis['ids_with_errors'][i])!='nan'):
            data[lis['agency_name'][i]][lis['form_name'][i]][lis['ids_with_errors'][i]]=False
    else:
        if(str(lis['ids_without_errors'][i])!='nan'):
            data[lis['agency_name'][i]][lis['form_name'][i]][lis['ids_without_errors'][i]]=True
        if(str(lis['ids_with_errors'][i])!='nan'):
            data[lis['agency_name'][i]][lis['form_name'][i]][lis['ids_with_errors'][i]]=False
#print(data)

#printing dictionary data

for i in data:
    print(i)
    for j in data[i]:
        print('    ',j)
        for x in data[i][j]:
            print('        ',x,data[i][j][x])

