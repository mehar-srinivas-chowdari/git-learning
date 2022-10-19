'''
import boto3
import time
import datetime

client = boto3.client('rds')

hostname = 'database-1.ct9uzzyvy4uv.us-east-1.rds.amazonaws.com'
port = 5432
user = 'sample_user'
region = 'us-east-1'

token = client.generate_db_auth_token(DBHostname = hostname , Port = port , DBUsername = user, Region = region)
'''
# retriving by parsing date_time and expiry_time from the token

def token_validation(token):
    ind = token.index('X-Amz-Date=')
    date_time_str = token[ind+11 : ind+26]

    exp = token.index('X-Amz-Expires=')
    expiry_val = int(token[exp+14 : exp+14+token[exp+14:].index('&')])

    ist = 19800  # neglect if using GMT
    ust = time.mktime(datetime.datetime.strptime(date_time_str,"%Y%m%dT%H%M%S").timetuple())
    creation_time = ust + ist

    expiry_time = creation_time + expiry_val

    current_time = int(datetime.datetime.now().timestamp())

    return current_time < expiry_time  # true if token is not expired

print(token_validation(token))
