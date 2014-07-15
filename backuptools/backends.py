from bakthat import backends
from filechunkio import FileChunkIO

__author__ = 'alex'


class S3BackendPlus(backends.S3Backend):

    def upload(self, keyname, filename, **kwargs):

        print(filename)

        raise Exception('task cancel')