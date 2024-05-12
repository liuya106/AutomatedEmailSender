import json
import urllib.parse
import boto3

print('Loading function')

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/337972440186/409a3unzippedqueue'

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    rawKey = event['Records'][0]['s3']['object']['key']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        
        print(bucket)
        print(rawKey)
        print(key)

        res = sqs.send_message(
            QueueUrl=queue_url,
            DelaySeconds=10,
            MessageBody=(key)
        )

        print(res['MessageId'])

        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

