import os
import sys

__author__ = 'alex'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # env setup
sys.path.append(os.path.join(BASE_DIR, "packages"))

import bakthat

from bakthat import app
from bakthat import CONFIG_FILE
from bakthat import _get_store_backend


@app.cmd(help="Synchronizes the local backup with the remote storage system.")
@app.cmd_arg('-s', '--source', type=str, help="localst", default='localst')
@app.cmd_arg('-d', '--destination', type=str, help="s3plus", default=None)
@app.cmd_arg('-c', '--config', type=str, default=CONFIG_FILE, help="path to config file")
@app.cmd_arg('-p', '--profile', type=str, default="default", help="profile name (default by default)")
def local_remote_sync(source='localst', destination=None, config=CONFIG_FILE, profile="default", **kwargs):

    local_storage_backend, source, conf = _get_store_backend(config, source, profile)
    remote_storage_backend, destination, _conf = _get_store_backend(config, destination, profile)

    for filename, stored_filename in local_storage_backend.ls():
        if remote_storage_backend.exists(stored_filename):
            continue

        # local file
        stored_filename_path = os.path.join(local_storage_backend.container,
                                            stored_filename)

        # only file
        if not os.path.isfile(stored_filename_path):
            raise NotImplemented

        root, ext = os.path.splitext(stored_filename_path)

        # clean name
        _stored_filename_path = os.path.join(local_storage_backend.container,
                                             filename + ("." if not ext.startswith('.') else ext))

        try:
            os.rename(stored_filename_path, _stored_filename_path)
            try:
                bakthat.backup(_stored_filename_path, destination=destination, config=config, profile=profile,
                               **kwargs)
            except Exception as e:
                bakthat.log.error('Failed Backup Upload: "{0}" | Reason: {1}'.format(
                    filename, str(e)))
        finally:
            if os.path.exists(_stored_filename_path):
                os.rename(_stored_filename_path, stored_filename_path)


if __name__ == '__main__':
    # run as command line program
    bakthat.main()

