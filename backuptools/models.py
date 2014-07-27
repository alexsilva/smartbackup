import hashlib
from bakthat import load_config
from bakthat.conf import config
import bakthat.models

__author__ = 'alex'


class Backups(bakthat.models.Backups):

    @classmethod
    def search(cls, query="", destination="", **kwargs):
        conf = config
        if kwargs.get("config"):
            conf = load_config(kwargs.get("config"))

        if not destination:
            destination = ["s3", "glacier"]
        if isinstance(destination, (str, unicode)):
            destination = [destination]

        query = "*{0}*".format(query)
        wheres = []

        if kwargs.get("profile"):
            profile = conf.get(kwargs.get("profile"))

            s3_key = hashlib.sha512(profile.get("access_key") +
                                    profile.get("s3_bucket")).hexdigest()

            localst_key = hashlib.sha512(profile.get("access_key") +
                                         profile.get("localst_path")).hexdigest()

            glacier_key = hashlib.sha512(profile.get("access_key") +
                                         profile.get("glacier_vault")).hexdigest()

            wheres.append(Backups.backend_hash << [s3_key, localst_key, glacier_key])

        wheres.append(Backups.filename % query | Backups.stored_filename % query)
        wheres.append(Backups.backend << destination)
        wheres.append(Backups.is_deleted == False)

        older_than = kwargs.get("older_than")
        if older_than:
            wheres.append(Backups.backup_date < older_than)

        backup_date = kwargs.get("backup_date")
        if backup_date:
            wheres.append(Backups.backup_date == backup_date)

        last_updated_gt = kwargs.get("last_updated_gt")
        if last_updated_gt:
            wheres.append(Backups.last_updated >= last_updated_gt)

        tags = kwargs.get("tags", [])
        if tags:
            if isinstance(tags, (str, unicode)):
                tags = tags.split()
            tags_query = ["Backups.tags % '*{0}*'".format(tag) for tag in tags]
            tags_query = eval("({0})".format(" and ".join(tags_query)))
            wheres.append(tags_query)

        return Backups.select().where(*wheres).order_by(Backups.last_updated.desc())