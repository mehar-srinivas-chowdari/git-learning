import csv

from infrastructure.aws.es import OpenSearchModule
from opensearchpy import OpenSearch
import pandas as pd

ES = OpenSearchModule()

hosts = ["http://localhost"]
port = 9200
auth = ("admin", "admin")

client = OpenSearch(
    hosts=hosts,
    port=port,
    http_auth=auth,
    use_ssl=True,
    verify_certs=False,
)

# mapping for document insertion

index_name = 'index_name'


index_body = {
    "mappings": {
        "properties": {
            "agencies": {
                "type": "nested",
                "properties": {
                    "agency_name": {"type": "text"},
                    "forms": {
                        "type": "nested",
                        "properties": {
                            "form_name": {"type": "text"},
                            "ids": {
                                "type": "nested",
                                "properties": {
                                    "id": {"type": "text"},
                                    "no_error": {"type": "boolean"}
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

if not client.indices.exists(index=index_name):
    response = client.indices.create(index_name, body=index_body)


def agy_exists(agy_details, agency_name, _str):
    for i in range(len(agy_details)):
        if agy_details[i][_str] == agency_name:
            return i
    return -1


def get_company_name(c):
    return "C" + str(c)


def insert_into_es(id_list, agency_name, form_name, index_name, c, bool):
    for main_id in id_list:
        c += 1
        company_name = "C" + str(c)
        # company_name = get_company_data_from_id(main_id, "State")
        # Search for the document.
        query = {
            "query": {
                "match": {
                    "company_name": company_name
                }
            }
        }
        response = client.search(body=query, index=index_name)
        if not response['hits']['hits']:
            document = {
                "company_name": company_name,
                "agencies": [
                    {
                        'agency_name': agency_name,
                        "forms": [
                            {'form_name': form_name,
                             'ids': [
                                 {"id": main_id,
                                  "no_error": bool
                                  }
                             ]
                             }
                        ]
                    }
                ]
            }
            client.index(index=index_name, body=document, id=company_name, refresh=True)
        else:
            document = response['hits']['hits'][0]['_source']
            agy_ind = agy_exists(document['agencies'], agency_name, 'agency_name')
            if agy_ind >= 0:
                form_ind = agy_exists(document['agencies'][agy_ind]['forms'], form_name, 'form_name')
                if form_ind >= 0:
                    document['agencies'][agy_ind]['forms'][form_ind] = {'form_name': form_name,
                                                                        'ids': [
                                                                            {"id": main_id, "no_error": bool}]}
                else:
                    document['agencies'][agy_ind]['forms'].append({'form_name': form_name,
                                                                   'ids': [
                                                                       {"id": main_id, "no_error": bool}]})
            else:
                document['agencies'].append({
                    'agency_name': agency_name,
                    'forms': [
                        {'form_name': form_name,
                         'ids': [
                             {"id": main_id, "no_error": bool}]}
                    ]
                })
            client.index(index=index_name, body=document, id=company_name, refresh=True)
    return c


df = pd.read_csv(
    r"C:\\Users\\banurusa\\IdeaProjects\\test_oc\\tests\\component_tests\\execution\\oc_run_level_status.csv")

# selecting only required fileds

df = df[['agency_name', 'ids_with_errors', 'ids_without_errors', 'form_name']]

# converting dataframe into dictonary of list

data = df.to_dict('list')
for i in range(len(data['agency_name'])):
    agency_name = data['agency_name'][i]
    form_name = data['form_name'][i]
    c = 0
    good_id_list = data['ids_without_errors'][i].strip('][').split(', ')
    bad_id_list = data['ids_with_errors'][i].strip('][').split(', ')
    if good_id_list != ['']:
        c = insert_into_es(good_id_list, agency_name, form_name, index_name, c, True)
    if bad_id_list != ['']:
        c = insert_into_es(bad_id_list, agency_name, form_name, index_name, c, False)


#querying and fetching results

query = {
    # "_source": ["company_name", "agencies.agency_name"],
    "query": {
        "bool": {
            "must_not": [
                {"nested": {
                    "path": "agencies",
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "nested": {
                                        "path": "agencies.forms",
                                        "query": {
                                            "bool": {
                                                "must": [
                                                    {
                                                        "nested": {
                                                            "path": "agencies.forms.ids",
                                                            "query": {
                                                                "bool": {
                                                                    "must": [
                                                                        {
                                                                            "match": {
                                                                                "agencies.forms.ids.no_error": False}
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
                }
            ]
        }
    }
}

response = client.search(
    body=query,
    index=index_name
)
print('\nSearch results: for all documents true')
print('We get response:', response)

print()


def create_csv_files(response, bool):
    heads1 = ['company_name']
    heads2 = ['company_name', "comp_id"]
    file1, file2, file3 = [], [], []
    for a in range(len(response['hits']['hits'])):
        data_dic = {}
        data_dic1 = {}
        fed_ids = []
        good_data_ids = []
        sou = response['hits']['hits'][a]['_source']
        file1.append({'company_name': sou['company_name']})
        data_dic['company_name'] = sou['company_name']
        for b in range(len(sou['agencies'])):
            if sou['agencies'][b]['agency_name'] not in heads2:
                heads2.append(sou['agencies'][b]['agency_name'])
            if sou['agencies'][b]['agency_name'] == "Federal":
                fed_ids.append(get_fed_id(sou['agencies'][b]))
            else:
                fed_ids.append("")
            data_form = sou['agencies'][b]['agency_name']
            data_ids = []
            data_dic[data_form] = []
            data_dic1[data_form] = []
            for c in range(len(sou['agencies'][b]['forms'])):
                if bool:
                    data_ids.append(sou['agencies'][b]['forms'][c]['form_name'])
                else:
                    for d in range(len(sou['agencies'][b]['forms'][c]['ids'])):
                        if sou['agencies'][b]['forms'][c]['ids'][d]['no_error']:
                            good_data_ids.append(sou['agencies'][b]['forms'][c]['form_name'])
                            data_dic1['company_name'] = sou['company_name']
                        else:
                            data_ids.append(sou['agencies'][b]['forms'][c]['form_name'])
            data_dic[data_form] += data_ids
            data_dic1[data_form] = good_data_ids if len(good_data_ids) > 0 else ""
        file2.append(data_dic)
        file3.append(data_dic1)

    if bool:
        return file1, file2, heads1, heads2
    return file1, file2, file3, heads1, heads2


def get_fed_id(_agency):
    return _agency['forms'][0]['ids'][0]['id']


file1, file2, heads1, heads2 = create_csv_files(response, True)
with open(r"C:\\Users\\banurusa\\IdeaProjects\\test_oc\\tests\\component_tests\\execution\\query_test\\good1.csv",
          'w') as f:
    writer = csv.DictWriter(f, fieldnames=heads1)
    writer.writeheader()
    writer.writerows(file1)

with open(r"C:\\Users\\banurusa\\IdeaProjects\\test_oc\\tests\\component_tests\\execution\\query_test\\good2.csv",
          'w') as f:
    writer = csv.DictWriter(f, fieldnames=heads2)
    writer.writeheader()
    writer.writerows(file2)
#

bad_id_query = {
    "query": {
        "bool": {
            "must": [
                {"nested": {
                    "path": "agencies",
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "nested": {
                                        "path": "agencies.forms",
                                        "query": {
                                            "bool": {
                                                "must": [
                                                    {
                                                        "nested": {
                                                            "path": "agencies.forms.ids",
                                                            "query": {
                                                                "bool": {
                                                                    "must_not": [
                                                                        {
                                                                            "match": {
                                                                                "agencies.forms.ids.no_error": True}
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
                }
            ]
        }
    }
}
response = client.search(
    body=bad_id_query,
    index=index_name
)

file1, file2, file3, heads1, heads2 = create_csv_files(response, False)
with open(r"C:\\Users\\banurusa\\IdeaProjects\\test_oc\\tests\\component_tests\\execution\\query_test\\bad1.csv",
          'w') as f:
    writer = csv.DictWriter(f, fieldnames=heads1)
    writer.writeheader()
    writer.writerows(file1)

with open(r"C:\\Users\\banurusa\\IdeaProjects\\test_oc\\tests\\component_tests\\execution\\query_test\\bad2.csv",
          'w') as f:
    writer = csv.DictWriter(f, fieldnames=heads2)
    writer.writeheader()
    writer.writerows(file2)

with open(r"C:\\Users\\banurusa\\IdeaProjects\\test_oc\\tests\\component_tests\\execution\\query_test\\bad3.csv",
          'w') as f:
    writer = csv.DictWriter(f, fieldnames=heads2)
    writer.writeheader()
    writer.writerows(file3)

file3, file4, heads1, heads2 = create_csv_files(response, False)
with open(r"C:\\Users\\banurusa\\IdeaProjects\\test_oc\\tests\\component_tests\\execution\\query_test\\bad1.csv",
          'w') as f:
    writer = csv.DictWriter(f, fieldnames=heads1)
    writer.writeheader()
    writer.writerows(file3)

with open(r"C:\\Users\\banurusa\\IdeaProjects\\test_oc\\tests\\component_tests\\execution\\query_test\\bad2.csv",
          'w') as f:
    writer = csv.DictWriter(f, fieldnames=heads2)
    writer.writeheader()
    writer.writerows(file4)
#
# print("**********good***********")
# print(file1)
# print(file2)
# print("***************************")
# print("*************bad***********")
# print(file3)
# print(file4)
# print("***************************")
