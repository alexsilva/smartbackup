import uuid
import sh
from bakthat import app, _get_store_backend, events, _interval_string_to_seconds, CONFIG_FILE, log
from models import Backups
from helper import BakHelper
from datetime import datetime

__author__ = 'alex'


@app.cmd(help="Delete backups older than the given interval string.")
@app.cmd_arg('filename', type=str, help="Filename to delete")
@app.cmd_arg('interval', type=str, help="Interval string like 1M, 1W, 1M3W4h2s")
@app.cmd_arg('-d', '--destination', type=str, help="s3|glacier|swift", default=None)
@app.cmd_arg('-p', '--profile', type=str, default="default", help="profile name (default by default)")
@app.cmd_arg('-c', '--config', type=str, default=CONFIG_FILE, help="path to config file")
def delete_older_filename(filename, interval, profile="default", config=CONFIG_FILE, destination=None, **kwargs):
    """Delete backups matching the given filename older than the given interval string.

    :type filename: str
    :param filename: File/directory name.

    :type interval: str
    :param interval: Interval string like 1M, 1W, 1M3W4h2s...
        (s => seconds, m => minutes, h => hours, D => days, W => weeks, M => months, Y => Years).

    :type destination: str
    :param destination: glacier|s3|swift

    :type conf: dict
    :keyword conf: Override/set AWS configuration.

    :rtype: list
    :return: A list containing the deleted keys (S3) or archives (Glacier).

    """
    storage_backend, destination, conf = _get_store_backend(config, destination, profile)

    session_id = str(uuid.uuid4())
    events.before_delete_older_than(session_id)

    interval_seconds = _interval_string_to_seconds(interval)
    backup_date_filter = int(datetime.utcnow().strftime("%s")) - interval_seconds

    deleted = []

    for backup in Backups.search_older_than(filename, backup_date_filter, destination=destination,
                                            profile=profile, config=config):
        real_key = backup.stored_filename
        log.info("Deleting {0}".format(real_key))

        storage_backend.delete(real_key)

        backup.set_deleted()
        deleted.append(backup)

    events.on_delete_older_than(session_id, deleted)

    return deleted



@app.cmd(help="Copies a dump of the database.")
@app.cmd_arg('-u', '--user', type=str, help='The database administrator.', default='root')
@app.cmd_arg('-p', '--password', type=str, help='User password', default='')
@app.cmd_arg('-b', '--backupname', type=str, help='Name (optional) backup file', default='host_mysql')
@app.cmd_arg('-host', '--host', type=str, help='Mysql host', default=None)
@app.cmd_arg('-d', '--destination', type=str, help="s3plus", default='s3plus')
@app.cmd_arg('-database', '--database', type=str)
@app.cmd_arg('--single-transaction', action='store_true', default=False, help='Lock mysql table.')
def mysqldump(user='root', password='', **kwargs):
    with BakHelper(kwargs['backupname'],
                   destination=kwargs['destination'],
                   password=password,
                   tags=["mysql"]) as bh:

        mysql_args = []

        mysql_kwargs = dict(
            u=user,
            B=kwargs['database'],
            _out="dump_{0}.sql".format(kwargs['database']))

        if kwargs.get('host') is not None:
            mysql_kwargs['h'] = kwargs['host']

        if kwargs.get('single_transaction'):
            mysql_args.append('--single-transaction')

        sh.mysqldump("-p{0}".format(password), *mysql_args, **mysql_kwargs)
        bh.backup()