import boto3
import sys
import os
from dotenv import load_dotenv
from os.path import dirname, join
from bucket_creater import BucketCreater
from export_data_source import ExportDataSource
from dataset_loader import DatasetLoader
from solution_creater import SolutionCreater
from campaign_creater import CampaignCreater
from recommend_getter import RecommendGetter
from sys import argv


def main():
    session = boto3.session.Session(
            profile_name=os.environ.get('PROFILE_NAME'))
    print('created session')

    BucketCreater(session).exec(_load_bucket_policy())
    print('created bucket: {}'.format(os.environ.get('BUCKET_NAME')))

    ExportDataSource(session).upload(
            join(dirname(__file__), 'ratings.csv'),
            'ratings.csv')
    print('uploaded csv')

    datalocation = 's3://{}/ratings.csv'.format(os.environ.get('BUCKET_NAME'))
    job_arn = DatasetLoader(session).import_job(
        _load_schema_definition(),
        {'dataLocation': datalocation}
    )
    print('dataset import job')

    solution_version_arn = SolutionCreater(session, job_arn).exec()
    print('solution created')

    campaign_arn = CampaignCreater(session, solution_version_arn).exec()

    print('success!')
    print('campaign arn is {}'.format(campaign_arn))

# private


def _load_pr_env():
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)


def _load_bucket_policy():
    bucket_policy_path = join(dirname(__file__), 'policy.json')
    policy = ''
    try:
        with open(bucket_policy_path) as file:
            policy = file.read()
            print('file loaded: {}'.format(policy))

    except IOError as error:
        print(error)
        sys.exit(-1)

    return policy


def _load_schema_definition():
    schema_definition_path = join(dirname(__file__), 'schema_definition.json')
    schema_definition = ''
    try:
        with open(schema_definition_path) as file:
            schema_definition = file.read()
            print('schema loaded: {}'.format(schema_definition))

    except IOError as error:
        print(error)
        sys.exit(-1)

    return schema_definition


_load_pr_env()
if len(argv) == 1:
    main()
# campaign_arn, user_id
elif len(argv) == 3:
    session = boto3.session.Session(
            profile_name=os.environ.get('PROFILE_NAME'))
    item_list = RecommendGetter(session, argv[1]).get(argv[2])
    print("🔻Recommended item ID")
    for item in item_list:
        print(item['itemId'])
        print('-------------')
