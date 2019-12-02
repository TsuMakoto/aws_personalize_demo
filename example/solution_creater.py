import time
import os
import sys


class SolutionCreater:
    def __init__(self, session, job_arn):
        self.personalize = session.client('personalize')
        self.name = os.environ.get('DATASET_SOLUTION_NAME')
        job = self.personalize.describe_dataset_import_job(
                datasetImportJobArn=job_arn)
        dataset = self.personalize.describe_dataset(
                datasetArn=job['datasetImportJob']['datasetArn'])
        self.dataset_group_arn = dataset['dataset']['datasetGroupArn']

    def exec(self):
        solution = self.personalize.create_solution(
                    name=self.name,
                    recipeArn='arn:aws:personalize:::recipe/aws-hrnn',
                    datasetGroupArn=self.dataset_group_arn
                )

        s = solution['solutionArn']
        description = self.personalize.describe_solution(solutionArn=s)
        # 100mまで待つ
        for i in range(60):
            if description['status'] == 'ACTIVE':
                break
            else:
                time.sleep(100)
            description = self.personalize.describe_solution(solutionArn=s)
        if description['status'] != 'ACTIVE':
            print('solution do not create')
            sys.exit(-1)

        version = self.personalize.create_solution_version(
                solutionArn=solution['solutionArn'])

        return version['solutionVersionArn']
