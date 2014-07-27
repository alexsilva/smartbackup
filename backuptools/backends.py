import mimetypes
import math
from multiprocessing.pool import ThreadPool
import os

from bakthat import backends, Backups
from bakthat.backends import BakthatBackend
from filechunkio import FileChunkIO


__author__ = 'alex'


class S3BackendPlus(backends.S3Backend):
    name = 's3plus'

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
            print "_upload_part(%s, %s, %s)" % (source_path, offset, bytes_len)

        def _upload(retries_left=amount_of_retries):
            try:
                if debug:
                    print 'Start uploading part #%d ...' % part_num

                for mp in self.bucket.get_all_multipart_uploads():
                    if mp.id == multipart_id:
                        with FileChunkIO(source_path, 'rb', offset=offset, bytes=bytes_len) as fp:
                            mp.upload_part_from_file(fp=fp, part_num=part_num,
                                                     cb=cb, num_cb=num_cb)
                        break
            except Exception, exc:
                if retries_left:
                    _upload(retries_left=retries_left - 1)
                else:
                    print 'Failed uploading part #%d' % part_num
                    raise exc
            else:
                if debug:
                    print '... Uploaded part #%d' % part_num

        _upload()

    def multipart_upload(self, keyname, source_path, source_size, **kwargs):
        acl = kwargs.get('acl', 'private')
        num_cb = kwargs.get('num_cb', 10)

        debug = kwargs.get('debug', True)
        headers = kwargs.get('headers', {})

        parallel_processes = kwargs.get('parallel_processes', 4)
        reduced_redundancy = kwargs.get('reduced_redundancy', False)

        if kwargs.get('guess_mimetype', True):
            mtype = mimetypes.guess_type(keyname)[0] or 'application/octet-stream'
            headers.update({'Content-Type': mtype})

        multipart_upload = self.bucket.initiate_multipart_upload(keyname, headers=headers,
                                                                 reduced_redundancy=reduced_redundancy)
        bytes_per_chunk = max(int(math.sqrt(5242880) * math.sqrt(source_size)),
                              5242880)
        chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))

        pool = ThreadPool(processes=parallel_processes)

        for index in range(chunk_amount):
            offset = index * bytes_per_chunk

            remaining_bytes = source_size - offset
            bytes_len = min([bytes_per_chunk, remaining_bytes])

            part_num = index + 1
            pool.apply_async(self._upload_part, [multipart_upload.id,
                                                 part_num, source_path, offset, bytes_len,
                                                 debug, self.cb, num_cb])
        pool.close()
        pool.join()

        if len(multipart_upload.get_all_parts()) == chunk_amount:
            multipart_upload.complete_upload()
            key = self.bucket.get_key(keyname)
            key.set_acl(acl)
        else:
            multipart_upload.cancel_upload()


class LocalStorageBackend(BakthatBackend):
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
        return [bk.stored_filename for bk in backups.order_by(Backups.last_updated.desc())]

    def delete(self, keyname):
        filepath = os.path.join(self.container, keyname)

        if os.path.isfile(filepath):
            os.remove(filepath)