import os


class ExportDataSource:
    def __init__(self, session):
        self.s3 = session.resource('s3')
        self.name = os.environ.get('BUCKET_NAME')

    def upload(self, src, dst):
        self.s3.meta.client.upload_file(src, self.name, dst)
