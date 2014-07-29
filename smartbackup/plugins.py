import peewee
from bakthat.plugin import Plugin
import bakthat

import backends
import models

import bakthat.models

__author__ = 'alex'


class MysqlEngineBackend(Plugin):

    def activate(self):
        #self.log.info("Connecting plugin '{0}'".format(backends.S3BackendPlus.name))
        #bakthat.models.database_proxy.initialize(peewee.MySQLDatabase(None))
        # bakthat.models.create_tables()
        pass


class S3BackendPlusPlugin(Plugin):

    def activate(self):
        #self.log.info("Connecting plugin '{0}'".format(backends.S3BackendPlus.name))
        bakthat.STORAGE_BACKEND[backends.S3BackendPlus.name] = backends.S3BackendPlus


class LocalBackendPlugin(Plugin):

    def activate(self):
        #self.log.info("Connecting plugin '{0}'".format(backends.LocalStorageBackend.name))
        bakthat.STORAGE_BACKEND[backends.LocalStorageBackend.name] = backends.LocalStorageBackend


class BackupsModelPlugin(Plugin):

    def activate(self):
        #self.log.info("Connecting plugin '{0}'".format(self.__class__.__name__))
        bakthat.Backups = models.Backups