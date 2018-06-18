
from __future__ import print_function

import boto3
import json
import botocore
import re
import pickle

ACCESS_KEY='AKIAJDGPCCUQUY6PMZGQ'
SECRET_KEY='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'
BUCKET_NAME='map.reducer'

s3 = boto3.resource('s3')
s3_cli =boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

def lambda_handler(event, context):
    f = s3.Object(BUCKET_NAME,event['key1'])
    n_mapper = event['key2']
    
    txt = re.sub('[^ a-zA-Z0-9]', ' ', f.get()['Body'].read())

    dict_map={}
    for word in txt.split():
        word=word.encode('utf-8')
        if word not in dict_map:
            dict_map[word] = 1
        else:
            dict_map[word] += 1

    pickle.dump(dict_map, open( "/tmp/save.txt", "wb"))

    s3_cli.upload_file("/tmp/save.txt", BUCKET_NAME, "save"+str(n_mapper)+".txt")