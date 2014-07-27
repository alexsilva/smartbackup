import os
import sys

__author__ = 'alex'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # env setup
sys.path.append(os.path.join(BASE_DIR, "packages"))

import bakthat  # lib import


if __name__ == '__main__':
    # run as command line program
    bakthat.main()

