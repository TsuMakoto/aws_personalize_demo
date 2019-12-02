class RecommendGetter:
    def __init__(self, session, campaign_arn):
        self.personalize_runtime = session.client('personalize-runtime')
        self.campaign_arn = campaign_arn

    def get(self, user_id):
        return self.personalize_runtime.get_recommendations(
                campaignArn=self.campaign_arn,
                userId=self.user_id,
                numResults=123)['itemList']
