import json, requests
from opensearchpy import OpenSearch

# Password for the 'elastic' user generated by Elasticsearch
PASS = "dmb6=xBEpXvFaMVRV4yU"

# Create the client instance

ca_certs_path = 'G:\elasticsearch-8.3.2\config\certs\http_ca.crt'

hosts = ["http://localhost"]
port = 9200
auth = ("elastic", PASS)

client = OpenSearch(
    hosts = hosts,
    port = port,
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,    
    ca_certs = ca_certs_path
)

# mapping for document insertion

index_name = 'excel_data'

index_body = {
  "mappings": {
    "properties": {
      "forms": {
        "type": "nested",
        "properties": {
          "ids": {
            "type": "nested"
          }
        }
      }
    }
  }
}

response = client.indices.create(index_name, body=index_body)


print('\nCreating index:')
print('We get response:', response)
Creating index:
We get response: {'acknowledged': True, 'shards_acknowledged': True, 'index': 'excel_data'}
from opensearchpy import OpenSearch

# Password for the 'elastic' user generated by Elasticsearch
PASS = "dmb6=xBEpXvFaMVRV4yU"

# Create the client instance

ca_certs_path = 'G:\elasticsearch-8.3.2\config\certs\http_ca.crt'
hosts = ["http://localhost"]
port = 9200
auth = ("elastic", PASS)

client = OpenSearch(
    hosts = hosts,
    port = port,
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,    
    ca_certs = ca_certs_path
)

import pandas as pd

df = pd.read_csv(r"G:\Downloads\oc_run_level_status.csv")

#selecting only required fileds

df = df[['agency_name','ids_with_errors','ids_without_errors','form_name']]

#converting dataframe into dictonary of list

data = df.to_dict('list')

def find_form(li,form_name):
    for i in range(len(li)):
        if(li[i]['form_name']==form_name):
            return i
    else:
        return -1

for i in range(len(data['agency_name'])):
    
    #DATA
    
    agency_name = data['agency_name'][i]
    form_name = data['form_name'][i]
    if(str(data['ids_without_errors'][i])!='nan'):
        tmp1={"id":data['ids_without_errors'][i],"no_error":True}
    else:
        tmp1={"id":None,"no_error":None}
    if(str(data['ids_with_errors'][i])!='nan'):
        tmp2={"id":data['ids_with_errors'][i],"no_error":False}
    else:
        tmp2={"id":None,"no_error":None}
    
    # Search for the document.
    
    index_name="excel_data"
    
    query = {
        "query":{
             "match":{
                  "agency_name": agency_name
              }
        }
    }
    response = client.search( body=query, index = index_name )
    
    
    
    if(response['hits']['hits']==[]):
        document = {
                    'agency_name':agency_name,
                    'forms':[
                        {
                         'form_name':form_name,
                         'ids':[tmp1,tmp2]
                        }]
                   }
        response_append = client.index(index = index_name, body = document, refresh = True)
    else:
        _id = response['hits']['hits'][0]['_id']
        document = response['hits']['hits'][0]['_source']
        ind = find_form(response['hits']['hits'][0]['_source']['forms'],form_name)

        if(ind>=0):
            document['forms'][ind]['ids']+=[tmp1,tmp2]
        else:
            document['forms'].append({"form_name":form_name,'ids':[tmp1,tmp2]})
            
        response_append = client.index(index = index_name, body = document,id= _id, refresh = True)
        
    print(response_append)
  
# SEARCH Query to find all forms with no errors

from opensearchpy import OpenSearch

# Password for the 'elastic' user generated by Elasticsearch
PASS = "dmb6=xBEpXvFaMVRV4yU"

# Create the client instance

ca_certs_path = 'G:\elasticsearch-8.3.2\config\certs\http_ca.crt'

hosts = ["http://localhost"]
port = 9200
auth = ("elastic", PASS)

client = OpenSearch(
    hosts = hosts,
    port = port,
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,    
    ca_certs = ca_certs_path
)

# Search for the document.

index_name = 'excel_data'

query = {
  "_source": ["agency_name"], 
  "query":{
    "bool":{
      "must_not":[
        {
          "nested": {
            "path": "forms",
            "query": {
              "bool": {
                "must":[
                  {
                    "nested":{
                      "path":"forms.ids",
                      "query":{
                        "bool": {
                          "must":[
                            {
                              "match":{"forms.ids.no_error":False}
                            }
                          ]
                        }
                      }
                    }
                  }
                ]
              }
            }
          }
        }
      ]
    }
  }
}
 
response = client.search(
    body = query,
    index = index_name
)
print('\nSearch results: for all documents true')
#print('We get response:', response)

print()

for a in range(len(response['hits']['hits'])):
    print(response['hits']['hits'][a]['_source']['agency_name'])
    
Search results: for all documents true

Connecticut
# Search for which id failed in which form

from opensearchpy import OpenSearch

# Password for the 'elastic' user generated by Elasticsearch
PASS = "dmb6=xBEpXvFaMVRV4yU"

# Create the client instance

ca_certs_path = 'G:\elasticsearch-8.3.2\config\certs\http_ca.crt'

hosts = ["http://localhost"]
port = 9200
auth = ("elastic", PASS)

client = OpenSearch(
    hosts = hosts,
    port = port,
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,    
    ca_certs = ca_certs_path
)

# Search for which id failed in which form.

index_name = 'excel_data'

query = {
  "_source":  ["agency_name"],
  "query":{
    "bool":{
      "must":[
        {
          "nested": {
            "path": "forms",
            "inner_hits": {
              "_source": ["forms.form_name"],
              "size": 20
            },  
            "query": {
              "bool": {
                "must":[
                  {
                    "nested":{
                      "path":"forms.ids",
                       "inner_hits": {
                          "_source":["forms.ids.id"],
                          "size": 100
                        },
                      "query":{
                        "bool": {
                          "must":[
                            {
                              "match":{"forms.ids.no_error":False}
                            }
                          ]
                        }
                      }
                    }
                  }
                ]
              }
            }
          }
        }
      ]
    }
  }
}
 
response = client.search(
    body = query,
    index = index_name
)
print('\nSearch results: for which ids failed in which forms')
#print('We get response:', response)


print()

for a in range(len(response['hits']['hits'])):
    for b in range(len(response['hits']['hits'][a]['inner_hits']['forms']['hits']['hits'])):
        print(response['hits']['hits'][a]['inner_hits']['forms']['hits']['hits'][b]['_source']['form_name'])
        x = response['hits']['hits'][a]['inner_hits']['forms']['hits']['hits'][b]
        for c in range(len(x['inner_hits']['forms.ids']['hits']['hits'])):
            print("    ",x['inner_hits']['forms.ids']['hits']['hits'][c]['_source']['id'])


