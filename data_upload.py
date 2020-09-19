# script to stream straight from URL to S3
# I've tried it with .csv files and it seems to work fine but 
# there were comments online that it garbled data so watch out
# Look here for more options: 
# https://stackoverflow.com/questions/14346065/upload-image-available-at-public-url-to-s3-using-boto

import requests
import boto3 # I had boto3 v1.15.0

url = 'url'
fname = 'destination filename and path'
bucket = 'bucket name'

s3_object = boto3.resource('s3').Object(bucket, name3)
with requests.get(url, stream=True) as r:
	s3_object.put(Body=r.content)