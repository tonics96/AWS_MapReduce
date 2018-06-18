import boto3
import botocore
import sys
import json
import os
import time


ACCESS_KEY=''
SECRET_KEY=''
BUCKET_NAME=''
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


for i in range(0,n):
	''' Cada mapper tendra el fichero que tiene que leer y el identificador de mapper. '''
	payload3["key2"]=i
	client.invoke(
		FunctionName="mapper",
		InvocationType='Event',
		Payload=json.dumps(payload3)
	)


time.sleep(20)


payload3={
	"key1":num_mappers
}


response=client.invoke(
	FunctionName="reducer",
	InvocationType='RequestResponse',
	Payload=json.dumps(payload3)
)


print response['Payload'].read()
