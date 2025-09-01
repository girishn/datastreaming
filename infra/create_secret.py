import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# Set your AWS region
region_name = 'us-east-1'  # Virginia

# Secret details
secret_name = "AlpacaApiSecret"

api_key = os.getenv("ALPACA_API_KEY")
api_secret = os.getenv("ALPACA_API_SECRET")

secret_value = '{"api_key": "' + api_key + '", "api_secret": "' + api_secret + '"}'

def create_secret():
    client = boto3.client('secretsmanager', region_name=region_name)

    try:
        response = client.create_secret(
            Name=secret_name,
            SecretString=secret_value
        )
        print(f"✅ Secret '{secret_name}' created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceExistsException':
            print(f"ℹ️ Secret '{secret_name}' already exists.")
        else:
            print(f"❌ Failed to create secret: {e}")
    except NoCredentialsError:
        print("❌ AWS credentials not found. Please configure them before running this script.")

if __name__ == "__main__":
    create_secret()
