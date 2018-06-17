
import boto3
import botocore
import json
import time

ACCESS_KEY='AKIAIB5G5CSABVQSLAIA'
SECRET_KEY='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'
BUCKET_NAME='toni3.mapreduce'
     
fitxer = 0
n_mappers = 0

while ((fitxer != '1') and (fitxer != '2') and (fitxer != '3')):
    fitxer = raw_input('\nQuin fitxer vols:\n1- Sherlock Holmes\n2- El Quijote\n3- The Bible\n')

if(fitxer == '1'):
    file = 'big.txt'
if(fitxer == '2'):
    file = 'pg2000.txt'
if(fitxer == '3'):
    file = 'pg10.txt'

#Estatico en un principio
n_mappers = 3

payload3 = dict()
payload3['key1'] = file
payload3['key2'] = n_mappers

s3=boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
client=boto3.client('lambda', region_name="eu-central-1", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

i = 0
nWords = 0
f = open(file, 'r')
for line in f:
	words = line.split(' ')
	for word in words:
		nWords += 1


payload3['key4'] = 0
payload3['key5'] = nWords/3-1
payload3['key6'] = nWords/3
payload3['key7'] = (nWords/3)*2
payload3['key8'] = (nWords/3)*2+1
payload3['key9'] = nWords

while (i<n_mappers):
	i += 1
	print i, n_mappers
	payload3['key3'] = i
	client.invoke(
		FunctionName="mapper2",
		InvocationType='Event',
		Payload=json.dumps(payload3)
	)
	
time.sleep(30)

response=client.invoke(
	FunctionName="reducer",
	InvocationType='RequestResponse',
	Payload=json.dumps(payload3)
)

print response['Payload'].read()