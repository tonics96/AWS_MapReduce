from __future__ import print_function

import boto3
import botocore
import re
import pickle

ACCESS_KEY='AKIAJQ3HE5DIINIL57QQ'
SECRET_KEY='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'
BUCKET_NAME='toni.mapreduce'


def lambda_handler(event, context):
	
	print 'entrando mapper!!'

	s3 = boto3.resource('s3')
	s3a =boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)


	count = 0
	file = s3.Object(BUCKET_NAME, event['key1'])
	num_mapper = event['key3']
	if(num_mapper = 1):
		inici = event['key4']
		final = event['key5']
	elif(num_mapper = 2):
		inici = event['key6']
		final = event['key7']
	else:
		inici = event['key8']
		final = event['key9']

	txt = re.sub('[^ a-zA-Z0-9]', ' ', file.get()['Body'].read())

	dict_map = {}

	for word in txt.split():
		word = word.encode('utf-8')
		if(count >= inici and count <= final):
			if word in dict_map:
				dict_map[word] += 1
			else:
				dict_map[word] = 1
		count += 1


	pickle.dump(dict_map, open("/tmp/mapper.txt", "wb"))
   	s3a.upload_file("/tmp/mapper.txt",BUCKET_NAME,"mapper"+str(num_mapper)+".txt")