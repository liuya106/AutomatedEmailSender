import json
import urllib.parse
import boto3
from botocore.exceptions import ClientError

print('Loading function')

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
ses = boto3.client('ses', region_name='us-east-1')
queue_url = 'https://sqs.us-east-1.amazonaws.com/337972440186/409a3countedqueue'

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    count = event['Records'][0]['messageAttributes']['Count']['stringValue']
    key = event['Records'][0]['body']
    #key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        
    body = "Unzipped file {} has a total of {} hello".format(key, count)
    print(body)
    try:
        response = ses.send_email(
                Destination={
                    'ToAddresses': [
                        'alexander.liu@mail.utoronto.ca',
                        'ahmed.qarmout@mail.utoronto.ca'
                    ],
                },
                Message={
                    'Body': {
                        'Text': {
                            'Charset': 'UTF-8',
                            'Data': body,
                        },
                    },
                    'Subject': {
                        'Charset': 'UTF-8',
                        'Data': 'A3HelloCount',
                    },
                },
                Source='Yang Liu <alexander.liu@mail.utoronto.ca>',
        )
        
        
        
    
    except ClientError as e:
        print(e.response['Error']['Message'])
        raise e
    else:
        response2 = sqs.receive_message(
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
        msg = response2['Messages'][0]
        receipt_handle = msg['ReceiptHandle']
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        
        print("Email sent! Message ID: "+ response['MessageId'])
