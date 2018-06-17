from __future__ import print_function

import boto3
import botocore
import pickle
import sys
import time

print('LOADING...')

ACCESS_KEY='AKIAJQ3HE5DIINIL57QQ'
SECRET_KEY='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'
BUCKET_NAME='toni.mapreduce'



def lambda_handler(event, context):
	s3a =boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
	final_map = {}
	response = dict()
	n_mappers = event['key2']
	i = 0
	count = event['key9']

	while (i<n_mappers):
		i += 1
		s3a.download_file(BUCKET_NAME, "mapper"+str(i)+".txt", "/tmp/mapper"+str(i)+".txt")

		tmp = pickle.load(open("/tmp/mapper"+str(i)+".txt", "rb"))

		tmp_dict = dict(tmp)

        for k in tmp_dict.keys():
            if final_map.has_key(k):
                final_map[k]+=tmp_dict.get(k)
            else:
                final_map[k]=tmp_dict.get(k)

	response['WordCount'] = count 
	response['CountingWords'] = final_map

	pickle.dump(response, open( "/tmp/final_result.txt", "wb" ))
	s3a.upload_file("/tmp/final_result.txt", BUCKET_NAME, "final_result.txt")
	return response