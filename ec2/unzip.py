import json
import urllib.parse
import boto3
import zipfile
from io import BytesIO
from time import sleep

s3 = boto3.resource('s3')
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/337972440186/409a3zippedqueue'



while True:
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )

    if 'Messages' not in response:
        print("Sleeping...")
        sleep(5)
        continue

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']

    key = message['Body']
    print('Zipped file name is ' + key)
    bucket = "409a3liuya106zipped"

    try:
        file = s3.Object(bucket, key)
        buffer = BytesIO(file.get()["Body"].read())
    
        z = zipfile.ZipFile(buffer)
        for filename in z.namelist():
            print("Unzipping %s" % filename)
            file_info = z.getinfo(filename)
            s3.meta.client.upload_fileobj(z.open(filename), '409a3liuya106unzipped', f'{filename}')

        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print('Received and deleted message: %s' % message)

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

