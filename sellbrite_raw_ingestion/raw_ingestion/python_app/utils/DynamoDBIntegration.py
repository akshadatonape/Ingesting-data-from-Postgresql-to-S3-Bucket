import boto3

def dynamodbConnection():

    TABLE_NAME = "dataset_info_dev"
    dynamodb = boto3.resource('dynamodb', region_name="us-west-2")
    table = dynamodb.Table(TABLE_NAME)
    response = table.scan()
    data = response['Items']
    return data