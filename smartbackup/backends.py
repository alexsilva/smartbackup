import mimetypes
import math
from multiprocessing.pool import ThreadPool
import os
import shutil

from bakthat import backends, Backups
from bakthat.backends import BakthatBackend, log
from boto.utils import compute_md5
from filechunkio import FileChunkIO


__author__ = 'alex'


class BaseBackend(BakthatBackend):
    def exists(self, store_filename):
        """
        Checks if the file already exists on the remote storage.
        :param store_filename: key of file
        """
        raise NotImplemented


class S3BackendPlus(backends.S3Backend, BaseBackend):
    name = 's3plus'

    def exists(self, store_filename):
        """ Checks if the file already exists on the remote storage. """
        return self.bucket.get_key(store_filename) is not None

    @staticmethod
    def _md5_checksum_metadata(source_path):
        checksum = {}
        with open(source_path, "rb") as fd:
            hex_digest, b64_digest, data_size = compute_md5(fd)
            checksum['b64_digest'] = b64_digest
        return checksum

    def upload(self, keyname, filename, **kwargs):
        source_size = os.stat(filename).st_size
        if source_size != 0:
            self.multipart_upload(keyname, filename, source_size, **kwargs)
        else:
            super(S3BackendPlus, self).upload(keyname, filename, **kwargs)

    def _upload_part(self, multipart_id, part_num, source_path, offset,
                     bytes_len, debug, cb, num_cb, amount_of_retries=10):
        """
        Uploads a part with retries.
        """
        if debug:
            log.info("_upload_part(%s, %s, %s)" % (source_path, offset, bytes_len))

        def _upload(retries_left=amount_of_retries):
            try:
                if debug:
                    log.info('Start uploading part #%d ...' % part_num)

                for mp in self.bucket.get_all_multipart_uploads():
                    if mp.id == multipart_id:
                        with FileChunkIO(source_path, 'rb', offset=offset, bytes=bytes_len) as fp:
                            mp.upload_part_from_file(fp=fp, part_num=part_num, cb=cb, num_cb=num_cb)
                        break
            except Exception as exc:
                if retries_left:
                    _upload(retries_left=retries_left - 1)
                else:
                    log.error('Failed uploading part #%d' % part_num)
                    log.exception(exc)
                    raise exc
            else:
                if debug:
                    log.info('... Uploaded part #%d' % part_num)

        _upload()

    def multipart_upload(self, keyname, source_path, source_size, **kwargs):
        acl = kwargs.pop('acl', 'private')
        num_cb = kwargs.pop('num_cb', 10)

        debug = kwargs.pop('debug', True)
        headers = kwargs.pop('headers', {})

        parallel_processes = kwargs.pop('parallel_processes', 4)
        reduced_redundancy = kwargs.pop('reduced_redundancy', False)

        if kwargs.get('guess_mimetype', True):
            mtype = mimetypes.guess_type(keyname)[0] or 'application/octet-stream'
            headers.update({'Content-Type': mtype})

        bytes_per_chunk = max(int(math.sqrt(5242880) * math.sqrt(source_size)),
                              5242880)
        chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))

        metadata = kwargs.pop('metadata', {})
        metadata.update(self._md5_checksum_metadata(source_path))
        multipart_upload = self.bucket.initiate_multipart_upload(keyname, headers=headers, metadata=metadata,
                                                                 reduced_redundancy=reduced_redundancy)

        pool = ThreadPool(processes=parallel_processes)

        for index in range(chunk_amount):
            offset = index * bytes_per_chunk

            remaining_bytes = source_size - offset
            bytes_len = min([bytes_per_chunk, remaining_bytes])

            part_num = index + 1

            # task args
            args = (
                multipart_upload.id,
                part_num,
                source_path,
                offset,
                bytes_len,
                debug,
                self.cb,
                num_cb
            )
            pool.apply_async(self._upload_part, args)

        pool.close()
        pool.join()

        parts = multipart_upload.get_all_parts()

        if parts is not None and len(parts) == chunk_amount:
            multipart_upload.complete_upload()
            key = self.bucket.get_key(keyname)
            key.set_acl(acl)
        else:
            multipart_upload.cancel_upload()


class LocalStorageBackend(BaseBackend):
    """ Backend to handle local storage. """
    name = "localst"

    def __init__(self, conf={}, profile="default"):
        BakthatBackend.__init__(self, conf, profile)

        self.container = self.conf["localst_path"]
        self.container_key = "localst_path"

        if not os.path.exists(self.container):
            os.makedirs(self.container)

    def download(self, keyname):
        encrypted_out = open(os.path.join(self.container, keyname), 'rb')
        return encrypted_out

    def upload(self, keyname, filename, **kwargs):
        shutil.copyfile(filename, os.path.join(self.container, keyname))

    def ls(self):
        backups = Backups.select().where(Backups.is_deleted == False, Backups.backend == self.name)
        return [backup.stored_filename for backup in backups.order_by(Backups.last_updated.desc())]

    def delete(self, keyname):
        filepath = os.path.join(self.container, keyname)

        if os.path.isfile(filepath):
            os.remove(filepath)