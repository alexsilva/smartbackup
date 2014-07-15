import os
import sys

__author__ = 'alex'

# env setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, "packages"))

# lib import
import bakthat
from backuptools import backends

bakthat.STORAGE_BACKEND['s3plus'] = backends.S3BackendPlus


if __name__ == '__main__':
    # run as command line program
    bakthat.main()

