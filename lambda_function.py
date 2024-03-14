import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import json
import logging
import os

"""
1- This function creates location data base of charging stations from all the vendors.
2- It will be called by cloudwatch periodically to update our aggregated DB
3- It will utilize --api url sender Lambda-- to fetch locations data
3- Currently It will check all vendors available in vendor db by default
check workflow
"""
# TODO: implement vendor specific sync also later

dynamo_client = boto3.resource(os.environ['DB_NAME'], region_name=os.environ['REGION_NAME'])
sns_client = boto3.client("sns")
s3_client = boto3.client("s3")

s3_vendor_param_object = s3_client.get_object(Bucket=os.environ['S3_BUCKET'], Key=os.environ['JSON_CONF'])
s3_vendor_param_data = json.loads(s3_vendor_param_object['Body'].read().decode('utf-8'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


def get_all_vendors():
    table = dynamo_client.Table(os.environ['DYNAMO_TABLE'])
    try:
        response = table.scan()
    except ClientError as e:
        logging.exception(f"Error in getting data from Vendor table -- {e.response['Error']['Message']}")
    else:
        return response['Items']


def start_station_location_gathering():
    vendors = [vendor["vendor_id"] for vendor in get_all_vendors()]
    logging.info(f"Here is the list of vendors {vendors}")

    for vendor in vendors:
        if vendor == "chargemod":
            payload = {"vendor_id": vendor, "action": "location",
                       "write": True,
                       "base_url": s3_vendor_param_data[vendor]["base_url"],
                       "params": s3_vendor_param_data[vendor]["params"],
                       "header": s3_vendor_param_data[vendor]["header"]}

            response = sns_client.publish(
                TopicArn=os.environ['SNS_TOPIC'],
                Message=json.dumps(payload)
            )
            return {'statusCode': 200,
                    'body': json.dumps(response)
                    }
        elif vendor == "other":
            pass
        else:
            pass


def lambda_handler(event, context):
    start_station_location_gathering()


if __name__ == '__main__':
    # write unit test for this function
    # first export all the env in local
    # then start the test
    pass

