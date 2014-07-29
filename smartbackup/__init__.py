import sh
from bakthat import app
from helper import BakHelper

__author__ = 'alex'


@app.cmd(help="Copies a dump of the database.")
@app.cmd_arg('-u', '--user', type=str, help='The database administrator.', default='root')
@app.cmd_arg('-p', '--password', type=str, help='User password', default='')
@app.cmd_arg('-b', '--backupname', type=str, help='Name (optional) backup file', default='host_mysql')
@app.cmd_arg('-d', '--destination', type=str, help="s3plus", default='s3plus')
@app.cmd_arg('-database', '--database', type=str)
def mysqldump(user='root', password='', **kwargs):
    with BakHelper(kwargs['backupname'], destination=kwargs['destination'],
                   password=password, tags=["mysql"]) as bh:
        sh.mysqldump("-p{0}".format(password), u=user, d=kwargs['database'],
                    _out="dump_{0}.sql".format(kwargs['database']))
        bh.backup()