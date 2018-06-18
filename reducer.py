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
PATH='/tmp/'


s3_b =boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)


def lambda_handler(event, context):
    final_map={}
    response=dict()
    n=event['Key1']
    contador=0
    
    for i in range(0,num):
      
        s3_b.download_file(BUCKET_NAME, "save"+str(i)+".txt", PATH+"save"+str(i)+".txt")
        
        tmp = pickle.load( open( PATH+"save"+str(i)+".txt", "rb" ) )
        
        tmp_dict=dict(tmp)
        
        
        if len(final_map)==0:
            final_map=tmp_dict
            for j in tmp_dict.keys():
                contador=contador+tmp_dict.get(j)
        
        else:
            for k in tmp_dict.keys():
                if final_map.has_key(k):
                    final_map[k]=final_map.get(k)+tmp_dict.get(k)
                else:
                    final_map[k]=tmp_dict.get(k)
                contador=contador+tmp_dict.get(k)

    
    response['map']=final_map
    response['counting']=contador

  
    pickle.dump(response, open( "/tmp/save_final.txt", "wb" ))

    
    s3_b.upload_file("/tmp/save_final.txt", BUCKET_NAME, "save_final.txt")

  
    return response
