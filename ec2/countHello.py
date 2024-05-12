import json
import urllib.parse
import boto3
import zipfile
from io import BytesIO
from time import sleep

s3 = boto3.resource('s3')
sqs = boto3.client('sqs', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/337972440186/409a3unzippedqueue'
target_url = 'https://sqs.us-east-1.amazonaws.com/337972440186/409a3countedqueue'
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('409a3count')


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
    bucket = "409a3liuya106unzipped"

    try:
        file = s3.Object(bucket, key)
        buffer = file.get()["Body"].read()
        count = buffer.decode("utf-8").count("hello")  
        print('Unzipped file {} has {} hello'.format(key, count))

        with table.batch_writer() as batch:
            batch.put_item(Item={"Filename": key, "Count": count})

        response = sqs.send_message(
            QueueUrl=target_url,
            DelaySeconds=10,
            MessageAttributes={
                'Count': {
                    'DataType': 'Number',
                    'StringValue': str(count)
                }
            },
            MessageBody=(key)
        )

        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print('Received and deleted message: %s' % message)

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

