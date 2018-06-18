import boto3
import botocore
import sys
import json
import os
import time


ACCESS_KEY='AKIAJDGPCCUQUY6PMZGQ'
SECRET_KEY='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'
BUCKET_NAME='map.reducer'
PATH='/home/milax/Desktop/SD_AWS/'
FILE=sys.argv[1]
num_mappers=int(sys.argv[2])
if num_mappers > 5:
	print "\nNo pueden haber mas de 5 mappers.\n"
	sys.exit(0)


if num_mappers < 1:
	print "\nNo pueden haber menos de 1 mappers.\n"
	sys.exit(0)


os.system("mkdir -p "+PATH)


payload3=dict()
payload3["key1"]=FILE


s3=boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)


client = boto3.client('lambda', region_name="eu-central-1", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)


for i in range(0,num_mappers):
	''' Cada mapper tendra el fichero que tiene que leer y el identificador de mapper. '''
	payload3["key2"]=i
	client.invoke(
		FunctionName="Mapper1",
		InvocationType='Event',
		Payload=json.dumps(payload3)
	)


time.sleep(20)


payload3={
	"key1":num_mappers
}


response=client.invoke(
	FunctionName="Reducer1",
	InvocationType='RequestResponse',
	Payload=json.dumps(payload3)
)


print response['Payload'].read()