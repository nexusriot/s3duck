import boto3
import botocore

from cryptography.fernet import Fernet


class Item:
    def __init__(self, name, type_, modified, size):
        self.name = name
        self.type_ = type_
        self.modified = modified
        self.size = size

    def __repr__(self):
        return "name: %s; type_: %d(%s), modified: %s size: %d" % (
            self.name,
            self.type_,
            "file" if self.type_ == 1 else "dir",
            self.modified,
            self.size
        )


class Model:
    def __init__(self, endpoint_url,
                 region_name,
                 encrypted_key_id,
                 encrypted_secret,
                 key,
                 bucket):

        self.session = boto3.session.Session()
        self._client = None
        self._fernet = None
        self.current_folder = ""
        self.prev_folder = ""
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.encrypted_key_id = encrypted_key_id
        self.encrypted_secret = encrypted_secret
        self.key = key
        self.bucket = bucket

    @staticmethod
    def generate_key():
        return Fernet.generate_key().decode()

    @property
    def fernet(self):
        if self._fernet is None:
            self._fernet = Fernet(self.key.encode())
        return self._fernet

    def decrypt_cred(self, val):
        return self.fernet.decrypt(val).decode()

    @property
    def client(self):
        if self._client is None:
            decrypted_key_id = self.decrypt_cred(self.encrypted_key_id)
            decrypted_key_secret_access_key = self.decrypt_cred(self.encrypted_secret)
            self._client = self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                config=botocore.config.Config(s3={'addressing_style': 'virtual'}),
                region_name=self.region_name,
                aws_access_key_id=decrypted_key_id,
                aws_secret_access_key=decrypted_key_secret_access_key
            )
        return self._client

    def list(self, fld):
        if fld:
            path = fld
        else:
            path = fld
        rsp = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=path,
            Delimiter='/')

        folders = [
            fld["Prefix"] for fld in rsp.get("CommonPrefixes", list())
        ]
        objects = [
            obj for obj in rsp.get("Contents", list())
        ]
        items = list()
        for folder in folders:
            s = folder.split("/")
            if len(s) > 1:
                folder = s[-2]
            items.append(
                Item(
                    folder,
                    2,
                    "",
                    0
                )
            )

        for obj in objects:
            key = obj["Key"]
            if key == path:
                continue
            filename = key.split("/")[-1]
            items.append(
                Item(
                    filename,
                    1,
                    obj['LastModified'],
                    obj['Size']
                ))
        return items

    def download_file(self, key, local_name):
        self.client.download_file(self.bucket, key, local_name)

