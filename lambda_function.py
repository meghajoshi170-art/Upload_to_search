import boto3
import requests
import json
import logging
from requests_aws4auth import AWS4Auth
from urllib.parse import quote_plus

# Config
REGION = 'us-east-1'
SERVICE = 'es'
HOST = 'https://vpc-opensearchdomain-tachsh6xhkkxruazssvq3j7qxy.aos.us-east-1.on.aws'
INDEX = 'opensearch'
DATATYPE = '_doc'
HEADERS = {"Content-Type": "application/json"}

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    REGION,
    SERVICE,
    session_token=credentials.token
)

def list_to_string(lines):
    return "".join([line.decode('utf-8', errors='ignore') if isinstance(line, bytes) else str(line) for line in lines])

def lambda_handler(event, context):
    logger.info("Lambda invoked")

    for record in event.get('Records', []):
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        logger.info("Fetching S3 object from bucket: %s, key: %s", bucket, key)

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            body = obj['Body'].read()
            lines = body.splitlines()
            logger.info(lines[0])
        except Exception as e:
            logger.error("Failed to fetch S3 object: %s", e)
            continue

        title = lines[0].decode('utf-8', errors='ignore') if len(lines) > 0 else ""
        author = lines[1].decode('utf-8', errors='ignore') if len(lines) > 1 else ""
        date = lines[2].decode('utf-8', errors='ignore') if len(lines) > 2 else ""
        content_lines = lines[3:] if len(lines) > 3 else []
        summary = content_lines[0].decode('utf-8', errors='ignore') if content_lines else ""
        print(title)
        print(author)
        print(date)
        print(content_lines)
        print(summary)
        document = {
            "Title": title,
            "Author": author,
            "Date": date,
            "Body": list_to_string(content_lines),
            "Summary": summary
        }
        
        logger.info("Document prepared: %s", document)
        
        # Post to OpenSearch
        doc_id = quote_plus(key)  # replaces /, spaces, etc.
        url = f"{HOST}/{INDEX}/{DATATYPE}/{doc_id}"
        try:
            response = requests.post(url, auth=awsauth, json=document, headers=HEADERS, timeout=10)
            logger.info("OpenSearch response: %s %s", response.status_code, response.text)
        except requests.exceptions.RequestException as e:
            logger.error("Failed to post to OpenSearch: %s", e)

    return {"status": "success"}
