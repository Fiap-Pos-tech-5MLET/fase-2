import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    job_name = 'glue-refined-zone-bovespa'
    response = boto3.client('glue').start_job_run(JobName=job_name)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
