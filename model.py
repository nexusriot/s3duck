import platform
import os
import boto3
import botocore


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
                 access_key,
                 secret_key,
                 bucket):

        self.session = boto3.session.Session()
        self._client = None
        self._fernet = None
        self.current_folder = ""
        self.prev_folder = ""
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket

    @staticmethod
    def get_os_family():
        return platform.system()

    @property
    def client(self):
        if self._client is None:
            self._client = self.session.client(
                's3',
                endpoint_url=self.endpoint_url,
                config=botocore.config.Config(s3={'addressing_style': 'virtual'}),
                region_name=self.region_name,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key
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

    def download_file(self, key: str, local_name: str, folder_path: str):
        if not local_name:
            keys = self.get_keys(key)
            for k, size in keys:
                rp = os.path.relpath(k, self.current_folder)
                path = os.path.join(folder_path, rp)
                if k.endswith("/"):
                    # make folder
                    os.makedirs(path, exist_ok=True)
                    continue
                # make sure directory exists before downloading
                os.makedirs(os.path.dirname(path), exist_ok=True)
                self.client.download_file(self.bucket, k, path)
        else:
            self.client.download_file(self.bucket, key, local_name)

    def create_folder(self, key):
        return self.client.put_object(Bucket=self.bucket, Key=key)

    def get_keys(self, prefix):
        r = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return [(key.get('Key'), key.get("Size")) for key in r.get("Contents", [])]

    def delete(self, key):
        # TODO: check usage
        if key.endswith("/"):
            keys = self.get_keys(key)
            for key, _ in keys:
                self.client.delete_object(Bucket=self.bucket, Key=key)
        else:
            self.client.delete_object(Bucket=self.bucket, Key=key)

    def upload_file(self, local_file, key):
        if local_file is None:
            self.create_folder("%s/" % key)
        else:
            self.client.upload_file(local_file, self.bucket, key)
