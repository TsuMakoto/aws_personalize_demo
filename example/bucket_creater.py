import os


class BucketCreater:
    def __init__(self, session, acl='private'):
        self.s3 = session.client('s3')
        self.name = os.environ.get('BUCKET_NAME')
        self.acl = os.environ.get('BUCKET_ACL')

    def exec(self, policy):
        self.s3.create_bucket(ACL=self.acl, Bucket=self.name)
        self.s3.put_bucket_policy(
                Bucket=self.name,
                ConfirmRemoveSelfBucketAccess=False,
                Policy=policy)
