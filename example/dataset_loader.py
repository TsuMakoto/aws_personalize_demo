import os
import sys
import time


class DatasetLoader:
    def __init__(self, session):
        self.personalize = session.client('personalize')
        self.dataset_group_name = os.environ.get('DATASET_GROUP_NAME')
        self.dataset_name = os.environ.get('DATASET_NAME')
        self.schema_name = os.environ.get('SCHEMA_NAME')
        self.job_name = os.environ.get('DATASET_IMPORT_JOB_NAME')
        self.role_arn = os.environ.get('ROLE_ARN')

    def import_job(self, schema_definition, data_source):
        dataset_group = self.personalize.create_dataset_group(
                name=self.dataset_name)
        dg_arn = dataset_group['datasetGroupArn']
        description = self.personalize.describe_dataset_group(
                    datasetGroupArn=dg_arn)['datasetGroup']

        # 60sec待つ
        print("creating dataset group")
        for i in range(60):
            if description['status'] == 'ACTIVE':
                break
            else:
                time.sleep(1)
                description = self.personalize.describe_dataset_group(
                            datasetGroupArn=dg_arn)['datasetGroup']
        if description['status'] != 'ACTIVE':
            print('dataset group not create')
            sys.exit(-1)

        schema = self.personalize.create_schema(
                name=self.schema_name,
                schema=schema_definition)

        dataset = self.personalize.create_dataset(
                name=self.dataset_name,
                schemaArn=schema['schemaArn'],
                datasetGroupArn=dataset_group['datasetGroupArn'],
                datasetType='Interactions')

        dataset_import_job = self.personalize.create_dataset_import_job(
                jobName=self.job_name,
                datasetArn=dataset['datasetArn'],
                dataSource=data_source,
                roleArn=self.role_arn)

        dij_arn = dataset_import_job['datasetImportJobArn']
        description = self.personalize.describe_dataset_import_job(
                    datasetImportJobArn=dij_arn
                )['datasetImportJob']
        # 100mまで待つ
        print("importing job")
        for i in range(60):
            if description['status'] == 'ACTIVE':
                break
            else:
                time.sleep(100)
                description = self.personalize.describe_dataset_import_job(
                        datasetImportJobArn=dij_arn
                    )['datasetImportJob']
        if description['status'] != 'ACTIVE':
            print('job do not success')
            sys.exit(-1)

        return dataset_import_job['datasetImportJob']
