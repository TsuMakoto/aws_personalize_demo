import os


class CampaignCreater:
    def __init__(self, session, solution_version):
        self.personalize = session.client('personalize')
        self.solution_version = solution_version
        self.name = os.environ.get('CAMPAIGN_NAME')

    def exec(self):
        return self.personalize.create_campaign(
                    name=self.name,
                    solutionVersionArn=self.solution_version,
                    minProvisionedTPS=123
                )['campaignArn']
