import bakthat.helper

__author__ = 'alex'


class BakHelper(bakthat.helper.BakHelper):

    def backup(self, filename=None, **kwargs):
        filename = self.tmpd if filename is None else filename

        return bakthat.backup(filename,
                              destination=kwargs.get("destination", self.destination),
                              prompt=kwargs.get("prompt", 'no'), # no ask
                              password=kwargs.get("password", None),
                              tags=kwargs.get("tags", self.tags),
                              profile=kwargs.get("profile", self.profile),
                              conf=kwargs.get("conf", self.conf),
                              key=kwargs.get("key", self.key),
                              custom_filename=self.backup_name)