from enum import Enum
import platform
import uuid
import os
import boto3
import botocore
from boto3.s3.transfer import TransferConfig


class FSObjectType(Enum):
    FILE = 1
    FOLDER = 2


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
            "file" if self.type_ == FSObjectType.FILE else "dir",
            self.modified,
            self.size,
        )


class _BotoProgressAdapter:
    """
    Adapt boto3 Callback(bytes_amount) -> progress_cb(total, current, key).
    """
    def __init__(self, total, key, cb):
        self.total = max(1, int(total or 0))
        self.key = key
        self.cb = cb
        self._sofar = 0

    def __call__(self, bytes_amount):
        self._sofar += int(bytes_amount or 0)
        if self.cb:
            cur = self._sofar if self._sofar <= self.total else self.total
            self.cb(self.total, cur, self.key)


class Model:
    def __init__(
        self,
        endpoint_url,
        region_name,
        access_key,
        secret_key,
        bucket,
        no_ssl_check,
        use_path,
        timeout=3,
        retries=3,
    ):
        self.session = boto3.session.Session()
        self._client = None
        self.current_folder = ""
        self.prev_folder = ""
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.no_ssl_check = no_ssl_check
        self.use_path = use_path
        self.timeout = timeout
        self.retries = retries

        # Smaller chunks -> more frequent callbacks -> smoother progress
        self.transfer_cfg = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,   # start multipart at 8MB
            multipart_chunksize=1 * 1024 * 1024,   # 1MB parts
            io_chunksize=256 * 1024,               # 256KB read size
            max_concurrency=4,
            use_threads=True,
        )

    @staticmethod
    def get_os_family():
        return platform.system()

    @property
    def client(self):
        if self._client is None:
            params = {
                "endpoint_url": self.endpoint_url,
                "aws_access_key_id": self.access_key,
                "aws_secret_access_key": self.secret_key,
            }
            if self.region_name:
                params.update({"region_name": self.region_name})

            s3_config = {"addressing_style": "virtual"} if not self.use_path else {"addressing_style": "path"}
            if self.no_ssl_check:
                params.update({"verify": False})

            params.update(
                {
                    "config": botocore.config.Config(
                        s3=s3_config,
                        connect_timeout=self.timeout,
                        retries={"max_attempts": self.retries},
                    ),
                }
            )
            self._client = self.session.client("s3", **params)
        return self._client

    def list(self, fld):
        path = fld or ""
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=path, Delimiter="/")

        items = []
        for page in pages:
            folders = [fld["Prefix"] for fld in page.get("CommonPrefixes", [])]
            objects = [obj for obj in page.get("Contents", [])]

            for folder in folders:
                s = folder.split("/")
                if len(s) > 1:
                    folder = s[-2]
                items.append(Item(folder, FSObjectType.FOLDER, "", 0))

            for obj in objects:
                key = obj["Key"]
                if key == path:
                    continue
                filename = key.split("/")[-1]
                items.append(Item(filename, FSObjectType.FILE, obj["LastModified"], obj["Size"]))
        return items

    def download_file(self, key: str, local_name: str, folder_path: str, progress_cb=None):
        """
        Download a single file or a whole prefix.
        - If local_name is truthy: download a single object to local_name.
        - Else: treat 'key' as a folder prefix and download whole tree into folder_path.

        progress_cb(total_bytes:int, downloaded_bytes:int, key:str) -> None
        """
        if not local_name:
            # Preserve the top-level folder: create base_dir/<folder> then put contents inside
            prefix = key if key.endswith("/") else key + "/"
            base_name = os.path.basename(prefix.rstrip("/"))
            base_dir = os.path.join(folder_path, base_name)
            os.makedirs(base_dir, exist_ok=True)

            for k, size in self.get_keys(prefix):
                rel = os.path.relpath(k, prefix)
                if rel == ".":  # placeholder object exactly equal to prefix
                    continue
                out_path = os.path.join(base_dir, rel)
                if k.endswith("/"):
                    os.makedirs(out_path, exist_ok=True)
                    continue
                os.makedirs(os.path.dirname(out_path), exist_ok=True)
                self.client.download_file(
                    self.bucket,
                    k,
                    out_path,
                    Callback=_BotoProgressAdapter(size, k, progress_cb),
                    Config=self.transfer_cfg,
                )
        else:
            # Single file
            size = None
            try:
                head = self.client.head_object(Bucket=self.bucket, Key=key)
                size = head.get("ContentLength")
            except Exception:
                pass
            self.client.download_file(
                self.bucket,
                key,
                local_name,
                Callback=_BotoProgressAdapter(size, key, progress_cb),
                Config=self.transfer_cfg,
            )

    def create_folder(self, key):
        return self.client.put_object(Bucket=self.bucket, Key=key)

    def get_keys(self, prefix):
        """
        Return [(Key, Size), ...] for ALL objects under 'prefix', paginated.
        Includes 'folder placeholder' keys (ending with '/').
        """
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        result = []
        for page in pages:
            for obj in page.get("Contents", []) or []:
                result.append((obj.get("Key"), obj.get("Size")))
        return result

    def delete(self, key) -> bool:
        if key.endswith("/"):
            for k, _ in self.get_keys(key):
                self.client.delete_object(Bucket=self.bucket, Key=k)
        else:
            self.client.delete_object(Bucket=self.bucket, Key=key)
        return True

    def upload_file(self, local_file, key, progress_cb=None):
        """
        Upload a file (with progress) or create a folder placeholder if local_file is None.

        progress_cb(total_bytes:int, uploaded_bytes:int, key:str) -> None
        """
        if local_file is None:
            self.create_folder("%s/" % key)
            return

        try:
            total = os.path.getsize(local_file)
        except Exception:
            total = None

        self.client.upload_file(
            local_file,
            self.bucket,
            key,
            Callback=_BotoProgressAdapter(total, key, progress_cb),
            Config=self.transfer_cfg,
        )

    def check_bucket(self):
        try:
            res = self.client.list_buckets()
            for b in res.get("Buckets", []):
                if b["Name"] == self.bucket:
                    return True, None
            reason = "bucket not found"
        except botocore.exceptions.ClientError as exc:
            reason = exc.response["Error"]["Message"]
        except Exception as exc:
            reason = str(exc)
        return False, reason

    def check_profile(self):
        res_c = res_d = False
        reason = None
        key = str(uuid.uuid4())
        try:
            try:
                res_c = self.create_folder(key)
            finally:
                res_d = self.delete(key)
        except botocore.exceptions.ClientError as exc:
            reason = exc.response["Error"]["Message"]
        except Exception as exc:
            reason = str(exc)
        return bool(res_c) and res_d, reason

    def get_size(self, key):
        # Sum sizes across full pagination
        total = 0
        for k, s in self.get_keys(key):
            if not k.endswith("/"):
                total += int(s or 0)
        return total

    def object_properties(self, key):
        return self.client.get_object(Bucket=self.bucket, Key=key)
