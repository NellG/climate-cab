# script to stream straight from URL to S3
# I've tried it with .csv files and it seems to work fine but 
# there were comments online that it garbled data so watch out
# Look here for more options: 
# https://stackoverflow.com/questions/14346065/upload-image-available-at-public-url-to-s3-using-boto

import requests
import boto3 # I had boto3 v1.15.0


def cab_data():
    """ Tranfer NYC TLC data to S3 bucket """
    years = ['2019']
    months = ['01', '02', '03', '04', '05', '06', 
              '07', '08', '09', '10', '11', '12']
    cats = [['grn', 'https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_'],
            ['ylw', 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_'],
            ['fhv', 'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_']]
            
    bucket = 'tlc-data-bucket'

    for y in years:
        for m in months:
            for c in cats:
                url = c[1] + y + '-' + m + '.csv'
                fname = y + '/' + c[0] + '_' + m + '.csv'
                print('Preparing to download from', url)
                s3_object = boto3.resource('s3').Object(bucket, fname)
                with requests.get(url, stream=True) as r:
                    s3_object.put(Body=r.content)
  

def main():  
    # Print names of all buckets to test connection to s3
    s3 = boto3.resource('s3')
    for b in s3.buckets.all():
        print(b.name)   
    
    #cab_data()


if __name__ == '__main__':
    main()