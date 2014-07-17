from bakthat.plugin import Plugin
import bakthat

import backends

__author__ = 'alex'


class S3BackendPlusPlugin(Plugin):

    def activate(self):
        self.log.info("Connecting plugin s3plus '{0}'".format(self.__class__.__name__))
        bakthat.STORAGE_BACKEND['s3plus'] = backends.S3BackendPlus