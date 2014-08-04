import mimetypes
import math
from multiprocessing.pool import ThreadPool
import os

from bakthat import backends, Backups
from bakthat.backends import BakthatBackend, log
from boto.utils import compute_md5
from filechunkio import FileChunkIO
from errors import UploadError
from smartbackup.utils import server_name_with

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

    def gen_keyname(self, keyname, path=None):
        keyname = server_name_with(self.conf, keyname)
        return (path + "/" + keyname) if path else keyname

    @staticmethod
    def _md5_checksum_metadata(source_path):
        checksum = {}
        with open(source_path, "rb") as _file:
            hex_digest, b64_digest, data_size = compute_md5(_file)
            checksum['b64_digest'] = b64_digest
        return checksum

    def download(self, keyname, **kwargs):
        keyname = self.gen_keyname(keyname, path=kwargs.pop('path', None))
        return super(S3BackendPlus, self).download(keyname)

    def upload(self, keyname, filename, **kwargs):
        keyname = self.gen_keyname(keyname, path=kwargs.pop('path', None))

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

    def get_multipart_upload_pending(self, keyname):
        """ pending upload operation """
        multipart_uploads = self.bucket.get_all_multipart_uploads()
        return [multipart_upload for multipart_upload in multipart_uploads if
                multipart_upload.key_name == keyname]

    @staticmethod
    def get_parts_range(multipart_upload, chunk_amount):
        amount_range = range(chunk_amount)
        uploads = [part.part_number-1 for part in multipart_upload.get_all_parts()]
        return filter(lambda n: n not in uploads, amount_range)

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

        multipart_upload_items = self.get_multipart_upload_pending(keyname)

        bytes_per_chunk = max(int(math.sqrt(5242880) * math.sqrt(source_size)),
                              5242880)
        chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))

        # start new upload
        if not any(multipart_upload_items):
            metadata = self._md5_checksum_metadata(source_path)
            multipart_upload = self.bucket.initiate_multipart_upload(keyname, headers=headers, metadata=metadata,
                                                                     reduced_redundancy=reduced_redundancy)
            multipart_upload_items = [multipart_upload]
        elif debug:
            log.info('Recover upload "{0}"'.format(keyname))

        # restart upload
        for multipart_upload in multipart_upload_items:
            pool = ThreadPool(processes=parallel_processes)

            for index in self.get_parts_range(multipart_upload, chunk_amount):
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

            if len(multipart_upload.get_all_parts()) == chunk_amount:
                multipart_upload.complete_upload()
                key = self.bucket.get_key(keyname)
                key.set_acl(acl)
            else:
                raise UploadError('Failed for now! Try Later.')


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
        filepath = os.path.join(self.container, keyname)

        with open(filepath, 'wb') as sfile:
            sfile.write(open(filename, 'rb').read())

    def ls(self):
        backups = Backups.select().where(Backups.is_deleted == False, Backups.backend == self.name)
        return [(bk.filename, bk.stored_filename) for bk in backups.order_by(Backups.last_updated.desc())]

    def delete(self, keyname):
        filepath = os.path.join(self.container, keyname)

        if os.path.isfile(filepath):
            os.remove(filepath)