
import boto3
import base64
from botocore.exceptions import ClientError
import sys
import json
class SecretsManager(object):

    def get_secret(self, secret_name, region_name):
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
            return get_secret_value_response['SecretString']
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])


secret_name = sys.argv[1]
region_name = "us-west-2"
Secrets = SecretsManager().get_secret(secret_name,region_name)
Secrets = json.loads(Secrets)
jdbcUsername = Secrets['username']
jdbcPassword = Secrets['password']
jdbcHostname = Secrets['host']
jdbcPort = Secrets['port']
jdbcDatabase = Secrets['dbname']
engine = 'postgresql'

def getJdbcUrl():

    jdbcUrl = f'jdbc:{engine}://{secret_name}:{jdbcPort}/{jdbcDatabase}'
    return jdbcUrl,jdbcUsername,jdbcPassword,secret_name