import os
import sys

__author__ = 'alex'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # env setup
sys.path.append(os.path.join(BASE_DIR, "packages"))

import sh
import bakthat

from bakthat import app
from backuptools.helper import BakHelper


@app.cmd(help="Copies a dump of the database.")
@app.cmd_arg('-u', '--user', type=str, help='The database administrator.', default='root')
@app.cmd_arg('-p', '--password', type=str, help='User password', default='')
@app.cmd_arg('-b', '--backupname', type=str, help='Name (optional) backup file', default='host_mysql')
@app.cmd_arg('-d', '--destination', type=str, help="s3plus", default='s3plus')
def mysql_dump(user='root', password='', **kwargs):
    with BakHelper(kwargs['backupname'], destination=kwargs['destination'],
                   password=password, tags=["mysql"]) as bh:
        sh.mysqldump("-p{0}".format(password), u=user,
                     all_databases=True,
                    _out="dump.sql")
        bh.backup()


if __name__ == '__main__':
    # run as command line program
    bakthat.main()

