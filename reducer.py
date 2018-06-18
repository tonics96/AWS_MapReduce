
from __future__ import print_function

import boto3
import json
import botocore
import re
import pickle

ACCESS_KEY='AKIAJDGPCCUQUY6PMZGQ'
SECRET_KEY='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'
BUCKET_NAME='map.reducer'
PATH='/tmp/'

s3_cli =boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

def lambda_handler(event, context):
    final_map={}
    response=dict()
    num=event['key1']
    count=0
    
    for i in range(0,num):
        s3_cli.download_file(BUCKET_NAME, "save"+str(i)+".txt", PATH+"save"+str(i)+".txt")
        f = pickle.load( open( PATH+"save"+str(i)+".txt", "rb" ) )
        dict_aux=dict(f)
        
        if len(final_map)==0:
            final_map=dict_aux
            for j in dict_aux.keys():
                count=count+dict_aux.get(j)
        else:
            for k in dict_aux.keys():
                if final_map.has_key(k):
                    final_map[k]=final_map.get(k)+dict_aux.get(k)
                else:
                    final_map[k]=m.get(k)
                count=count+dict_aux.get(k)

    response['map']=final_map
    response['count']=count

    pickle.dump(response, open( "/tmp/save_final.txt", "wb" ))

    s3_cli.upload_file("/tmp/save_final.txt", BUCKET_NAME, "save_final.txt")

    return response