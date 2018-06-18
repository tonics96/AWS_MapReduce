from __future__ import print_function

import boto3
import json
import botocore
import re
import pickle

print('Loading function')


ACCESS_KEY=''
SECRET_KEY=''
BUCKET_NAME=''

s3 = boto3.resource('s3')
s3_b =boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)


def lambda_handler(event, context):
   
    fitxer = s3.Object(BUCKET_NAME,event['key1'])
    num_mappers = event['key2']
    
   
    txt = re.sub('[^ a-zA-Z0-9]', ' ', fitxer.get()['Body'].read())

    
    dict_map={}
    for word in txt.split():
        word=word.encode('utf-8')
        if word not in dict_map:
            dict_map[word] = 1
        else:
            dict_map[word] += 1

   
    pickle.dump(dict_map, open( "/tmp/save.txt", "wb"))

    
    s3_b.upload_file("/tmp/save.txt", BUCKET_NAME, "save"+str(num_mappers)+".txt")
