"""s3/binding.py: clients, endpoints, regions, profiles."""

import diagnostics
import model as model_module
import os
import re
import tempfile
import threading
import unittest
from botocore.awsrequest import AWSResponse
from botocore.compat import HTTPHeaders
from core.endpoints import aws_endpoint_region, strip_bucket_label
from model import (
    Model, RateLimiter, ReadOnlyError, TransferCancelled, run_parallel,
)
from tests.support import (
    FakeS3Client, PROJECT_ROOT, _StubClient, _ensure_qapp, make_model,
)
from urllib.parse import parse_qs, urlparse
from utils import load_aws_profiles


class BuildRegionSwappedEndpointTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model()

    def test_bare_s3(self):
        self.assertEqual(
            self.m.build_region_swapped_endpoint("https://s3.amazonaws.com", "eu-north-1"),
            "https://s3.eu-north-1.amazonaws.com",
        )

    def test_regioned_s3(self):
        self.assertEqual(
            self.m.build_region_swapped_endpoint("https://s3.eu-central-1.amazonaws.com", "eu-north-1"),
            "https://s3.eu-north-1.amazonaws.com",
        )

    def test_scheme_and_port_preserved(self):
        self.assertEqual(
            self.m.build_region_swapped_endpoint("http://s3.us-west-2.amazonaws.com:9000", "eu-north-1"),
            "http://s3.eu-north-1.amazonaws.com:9000",
        )

    def test_non_aws_returns_none(self):
        self.assertIsNone(self.m.build_region_swapped_endpoint("https://minio.local:9000", "eu-north-1"))
        self.assertIsNone(self.m.build_region_swapped_endpoint("", "eu-north-1"))
class EndpointHasBucketTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model()

    def test_virtual_host_prefix(self):
        self.assertTrue(self.m._endpoint_has_bucket("https://mybucket.s3.amazonaws.com", "mybucket"))
        self.assertTrue(self.m._endpoint_has_bucket("https://mybucket.s3.amazonaws.com", "MYBUCKET"))

    def test_not_present(self):
        self.assertFalse(self.m._endpoint_has_bucket("https://s3.amazonaws.com", "mybucket"))
        self.assertFalse(self.m._endpoint_has_bucket("https://other.s3.amazonaws.com", "mybucket"))

    def test_extract_leftmost_label(self):
        self.assertEqual(self.m._extract_leftmost_label("https://mybucket.s3.amazonaws.com"), "mybucket")
        self.assertEqual(self.m._extract_leftmost_label("https://s3.amazonaws.com"), "s3")
        self.assertEqual(self.m._extract_leftmost_label(""), "")
class AwsEndpointRegionTests(unittest.TestCase):
    def test_regional_hosts(self):
        for url, region in (
                ("https://s3.eu-west-1.amazonaws.com", "eu-west-1"),
                ("https://mybucket.s3.eu-west-1.amazonaws.com", "eu-west-1"),
                ("https://s3-eu-west-1.amazonaws.com", "eu-west-1"),
                ("https://s3.dualstack.us-gov-west-1.amazonaws.com",
                 "us-gov-west-1"),
                ("https://s3.cn-north-1.amazonaws.com.cn", "cn-north-1")):
            with self.subTest(url=url):
                self.assertEqual(aws_endpoint_region(url), region)

    def test_the_global_endpoint_is_us_east_1(self):
        self.assertEqual(aws_endpoint_region("https://s3.amazonaws.com"),
                         "us-east-1")
        self.assertEqual(
            aws_endpoint_region("https://mybucket.s3.amazonaws.com"),
            "us-east-1")

    def test_hosts_that_name_no_region(self):
        for url in ("https://s3-accelerate.amazonaws.com",
                    "https://s3-external-1.amazonaws.com",
                    "https://minio.local:9000", ""):
            with self.subTest(url=url):
                self.assertEqual(aws_endpoint_region(url), "")


class StripBucketLabelTests(unittest.TestCase):
    def test_a_leading_bucket_label_comes_off(self):
        self.assertEqual(
            strip_bucket_label(
                "https://mybucket.s3.eu-west-1.amazonaws.com", "mybucket"),
            "https://s3.eu-west-1.amazonaws.com")

    def test_scheme_and_port_survive(self):
        self.assertEqual(
            strip_bucket_label("http://MyBucket.minio.local:9000", "mybucket"),
            "http://minio.local:9000")

    def test_a_bucket_named_like_a_region_is_not_cut_from_the_middle(self):
        url = "https://s3.eu-west-1.amazonaws.com"
        self.assertEqual(strip_bucket_label(url, "eu-west-1"), url)

    def test_a_service_host_is_left_alone(self):
        url = "https://s3.eu-west-1.amazonaws.com"
        self.assertEqual(strip_bucket_label(url, "mybucket"), url)


class SessionTokenTests(unittest.TestCase):
    """Temporary credentials (STS / SSO / assumed role) need a session token;
    without one those credentials cannot be used at all."""

    def test_token_is_passed_to_the_client(self):
        captured = {}
        m = make_model(session_token="TOKEN")

        class FakeSession:
            def client(self, _name, **kw):
                captured.update(kw)
                return _StubClient()

        m.session = FakeSession()
        m._make_client()
        self.assertEqual(captured["aws_session_token"], "TOKEN")

    def test_absent_token_is_omitted(self):
        captured = {}
        m = make_model()

        class FakeSession:
            def client(self, _name, **kw):
                captured.update(kw)
                return _StubClient()

        m.session = FakeSession()
        m._make_client()
        self.assertNotIn("aws_session_token", captured)

    def test_every_client_gets_the_ranged_checksum_guard(self):
        """It is not optional the way requester-pays is: a client built
        without it cannot read a byte range on some backends at all."""
        m = make_model()

        class FakeSession:
            def client(self, _name, **kw):
                return _StubClient()

        m.session = FakeSession()
        client = m._make_client()
        self.assertIn("before-call.s3.GetObject", client.registered)

    def test_clone_carries_the_token(self):
        m = make_model(session_token="TOKEN")
        self.assertEqual(m.clone_for_worker().session_token, "TOKEN")
class LoadAwsProfilesTests(unittest.TestCase):
    def _write(self, tmp, name, text):
        path = os.path.join(tmp, name)
        with open(path, "w") as handle:
            handle.write(text)
        return path

    def test_credentials_and_config_are_merged(self):
        with tempfile.TemporaryDirectory() as tmp:
            creds = self._write(tmp, "credentials", """
[default]
aws_access_key_id = AKDEFAULT
aws_secret_access_key = SECRET1

[work]
aws_access_key_id = AKWORK
aws_secret_access_key = SECRET2
aws_session_token = TOKEN2
""")
            config = self._write(tmp, "config", """
[default]
region = eu-north-1

[profile work]
region = us-west-2
endpoint_url = https://minio.local:9000
""")
            profiles = load_aws_profiles(creds, config)
            self.assertEqual(set(profiles), {"default", "work"})
            self.assertEqual(profiles["default"]["access_key"], "AKDEFAULT")
            self.assertEqual(profiles["default"]["region"], "eu-north-1")
            self.assertEqual(profiles["default"]["session_token"], "")
            self.assertEqual(profiles["work"]["session_token"], "TOKEN2")
            self.assertEqual(profiles["work"]["region"], "us-west-2")
            self.assertEqual(
                profiles["work"]["endpoint_url"], "https://minio.local:9000")

    def test_config_only_profiles_are_dropped(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = self._write(tmp, "config", "[profile empty]\nregion = x\n")
            self.assertEqual(load_aws_profiles(os.path.join(tmp, "none"), config), {})

    def test_missing_files_are_not_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                load_aws_profiles(os.path.join(tmp, "a"), os.path.join(tmp, "b")),
                {},
            )
class BindingCacheTests(unittest.TestCase):
    """Opening an off-region bucket probes several endpoint/style combos; a
    proven binding must be reused instead of re-probed."""

    def setUp(self):
        self.m = make_model()
        self.made = []

        def _make_client(**kw):
            self.made.append(kw)
            return FakeS3Client(list_pages=[{}])

        self.m._make_client = _make_client

    def test_probe_result_is_cached_and_reused(self):
        probes = []

        def _probe(name):
            probes.append(name)
            return FakeS3Client(list_pages=[{}]), "https://ep", "eu-north-1", True

        self.m._probe_bucket_binding = _probe

        first = self.m._try_bind_bucket("b1")
        self.assertEqual(first[1:], ("https://ep", "eu-north-1", True))
        self.assertEqual(probes, ["b1"])
        self.assertEqual(
            self.m.binding_cache["b1"], ("https://ep", "eu-north-1", True))

        # second open: cached combo is used, no re-probe
        again = self.m._try_bind_bucket("b1")
        self.assertEqual(again[1:], ("https://ep", "eu-north-1", True))
        self.assertEqual(probes, ["b1"])
        self.assertEqual(self.made[-1]["endpoint_url"], "https://ep")

    def test_stale_entry_is_dropped_and_reprobed(self):
        probes = []

        def _probe(name):
            probes.append(name)
            return FakeS3Client(list_pages=[{}]), "https://new", "us-east-1", False

        self.m._probe_bucket_binding = _probe
        self.m.binding_cache["b1"] = ("https://stale", "eu-west-1", True)
        # a cached client that cannot list forces a re-probe
        self.m._listing_headers = lambda client, bucket: None

        result = self.m._try_bind_bucket("b1")
        self.assertEqual(result[1:], ("https://new", "us-east-1", False))
        self.assertEqual(probes, ["b1"])
        self.assertEqual(
            self.m.binding_cache["b1"], ("https://new", "us-east-1", False))

    def test_clone_shares_the_cache(self):
        self.m.binding_cache["b1"] = ("https://ep", "r", True)
        clone = self.m.clone_for_worker()
        clone.binding_cache["b2"] = ("https://ep2", "r2", False)
        # discoveries made by a worker are not lost
        self.assertIn("b2", self.m.binding_cache)
class _Raw:
    def __init__(self, body):
        self._body = body

    def stream(self, **_kwargs):
        yield self._body


class _FakeAws:
    """
    Just enough of AWS's region behaviour to meet botocore's own S3
    redirector, which a FakeS3Client never does: the bug it hid is a request
    that is refused for its region, re-signed by botocore, and then succeeds.

    Hooked in at before-send, so every request goes through real signing and
    real retry handling and stops just short of the network. Hosts outside
    amazonaws.com behave like a MinIO with no region configured: any signing
    region is accepted and none is ever named.
    """

    _REGIONAL = re.compile(r"(?:^|\.)s3\.([a-z0-9-]+)\.amazonaws\.com$")

    def __init__(self, regions, *, listing_names_region=True,
                 refuse_global_host=False):
        self.regions = dict(regions)
        self.listing_names_region = listing_names_region
        # A brand-new bucket is not reachable through s3.amazonaws.com in
        # either style until DNS catches up; the redirect hint is then the
        # only way in.
        self.refuse_global_host = refuse_global_host
        self.requests = []          # (method, url, signing region)

    def attach(self, model):
        make = model._make_client

        def _make_client(**kw):
            client = make(**kw)
            client.meta.events.register("before-send.s3", self._send)
            return client

        model._make_client = _make_client
        return model

    def urls(self):
        return [url for _method, url, _region in self.requests]

    @staticmethod
    def _response(request, status, headers, body=b""):
        h = HTTPHeaders()
        for name, value in headers.items():
            h[name] = value
        if request.method == "HEAD":
            body = b""
        return AWSResponse(request.url, status, h, _Raw(body))

    def _send(self, request, **_kwargs):
        url = urlparse(request.url)
        host = url.hostname
        auth = request.headers.get("Authorization") or b""
        auth = auth.decode() if isinstance(auth, bytes) else auth
        signed = re.search(r"Credential=[^/]+/\d+/([^/]+)/", auth).group(1)
        self.requests.append((request.method, request.url, signed))

        bucket = next((b for b in self.regions if host.startswith(b + ".")),
                      None)
        virtual = bucket is not None
        path = url.path.lstrip("/")
        if not virtual:
            bucket, _, path = path.partition("/")
        region = self.regions[bucket]

        if not host.endswith(".amazonaws.com"):
            return self._listing(request, bucket, path, {})

        named = {"x-amz-bucket-region": region}
        is_global = host == "s3.amazonaws.com" or host.endswith(
            ".s3.amazonaws.com")
        match = self._REGIONAL.search(host)
        wrong_host = (
            (is_global and region != "us-east-1"
             and (not virtual or self.refuse_global_host))
            or (match is not None and match.group(1) != region))
        if wrong_host:
            return self._response(request, 301, named, (
                "<Error><Code>PermanentRedirect</Code><Message>Use the "
                "specified endpoint.</Message>"
                f"<Endpoint>{bucket}.s3.{region}.amazonaws.com</Endpoint>"
                f"<Bucket>{bucket}</Bucket></Error>").encode())
        if signed != region:
            return self._response(request, 400, named, (
                "<Error><Code>AuthorizationHeaderMalformed</Code>"
                f"<Message>the region '{signed}' is wrong; expecting "
                f"'{region}'</Message><Region>{region}</Region>"
                "</Error>").encode())
        if request.method == "HEAD":
            return self._response(request, 200, named)
        return self._listing(
            request, bucket, path,
            named if self.listing_names_region else {})

    def _listing(self, request, bucket, path, headers):
        if path:
            # On a virtual host anything in the path is a KEY: the bucket
            # named twice is a GET of an object, not a listing.
            return self._response(request, 404, headers, (
                "<Error><Code>NoSuchKey</Code><Message>The specified key "
                "does not exist.</Message></Error>").encode())
        if request.method == "HEAD":
            return self._response(request, 200, headers)
        return self._response(
            request, 200, dict(headers, **{"Content-Type": "application/xml"}),
            (f"<ListBucketResult><Name>{bucket}</Name><Prefix></Prefix>"
             "<KeyCount>0</KeyCount><MaxKeys>1</MaxKeys>"
             "<Delimiter>/</Delimiter><IsTruncated>false</IsTruncated>"
             "</ListBucketResult>").encode())


def _credential_region(presigned_url):
    """The region in a presigned URL's credential scope."""
    query = parse_qs(urlparse(presigned_url).query)
    return query["X-Amz-Credential"][0].split("/")[2]


class OffRegionBucketTests(unittest.TestCase):
    """A profile on the global endpoint (us-east-1) opening a bucket in
    eu-west-1. botocore's redirector made the probe succeed with the
    profile's region, so that region was bound and cached: listing kept
    working, but every presigned link was signed for us-east-1 and AWS
    refused it, and the direct link was a path-style URL on the global host,
    which is a PermanentRedirect for that bucket."""

    def setUp(self):
        self.aws = _FakeAws({"eu-bucket": "eu-west-1",
                             "us-bucket": "us-east-1"})
        self.m = self.aws.attach(make_model())

    def test_the_presigned_get_url_is_signed_for_the_buckets_region(self):
        self.m.enter_bucket("eu-bucket")
        url = self.m.presigned_get_url("report.pdf")
        self.assertEqual(_credential_region(url), "eu-west-1")
        self.assertEqual(urlparse(url).hostname,
                         "eu-bucket.s3.eu-west-1.amazonaws.com")

    def test_the_presigned_put_url_is_signed_for_the_buckets_region(self):
        self.m.enter_bucket("eu-bucket")
        url = self.m.presigned_put_url("incoming/upload.bin")
        self.assertEqual(_credential_region(url), "eu-west-1")

    def test_the_direct_link_is_on_the_buckets_regional_endpoint(self):
        self.m.enter_bucket("eu-bucket")
        self.assertEqual(self.m.direct_object_url("a b.txt"),
                         "https://s3.eu-west-1.amazonaws.com/eu-bucket/a%20b.txt")

    def test_the_cache_holds_the_buckets_region_and_does_not_flip_back(self):
        self.m.enter_bucket("eu-bucket")
        proven = ("https://s3.eu-west-1.amazonaws.com", "eu-west-1", False)
        self.assertEqual(self.m.binding_cache["eu-bucket"], proven)

        # Back to the bucket list (the profile's own endpoint and region, as
        # the navigation code restores them) and in again.
        self.m.endpoint_url = self.m.profile_endpoint_url
        self.m.region_name = self.m.profile_region
        self.m._client = None
        del self.aws.requests[:]
        self.m.enter_bucket("eu-bucket")

        self.assertEqual(self.m.binding_cache["eu-bucket"], proven)
        self.assertEqual(self.m.region_name, "eu-west-1")
        # One listing, signed right first time: nothing re-probed, nothing
        # redirected.
        self.assertEqual(self.aws.requests, [(
            "GET",
            "https://eu-bucket.s3.eu-west-1.amazonaws.com/?list-type=2"
            "&prefix=&delimiter=%2F&encoding-type=url",
            "eu-west-1")])

    def test_a_cached_entry_with_the_profiles_region_heals(self):
        """What earlier versions persisted for such a bucket. It lists fine
        for ever, so only reading the region off that listing can fix it."""
        self.m.binding_cache["eu-bucket"] = (
            "https://s3.amazonaws.com", "us-east-1", False)
        self.m.enter_bucket("eu-bucket")
        self.assertEqual(
            self.m.binding_cache["eu-bucket"],
            ("https://s3.eu-west-1.amazonaws.com", "eu-west-1", False))
        self.assertEqual(
            _credential_region(self.m.presigned_get_url("k")), "eu-west-1")

    def test_a_path_style_profile_is_corrected_too(self):
        # Path style on the global host is a PermanentRedirect botocore
        # cannot follow; the flipped style is what gets redirected into
        # "working" with the wrong region.
        m = self.aws.attach(make_model(use_path=True))
        m.enter_bucket("eu-bucket")
        self.assertEqual(m.region_name, "eu-west-1")
        self.assertEqual(m.endpoint_url, "https://s3.eu-west-1.amazonaws.com")
        self.assertEqual(_credential_region(m.presigned_get_url("k")),
                         "eu-west-1")

    def test_a_bucket_in_the_profiles_region_costs_nothing_extra(self):
        self.m.enter_bucket("us-bucket")
        self.assertEqual(self.m.binding_cache["us-bucket"],
                         ("https://s3.amazonaws.com", "us-east-1", False))
        self.assertEqual(len(self.aws.requests), 1)

    def test_head_bucket_answers_when_the_listing_does_not_name_a_region(self):
        aws = _FakeAws({"eu-bucket": "eu-west-1"}, listing_names_region=False)
        m = aws.attach(make_model())
        m.enter_bucket("eu-bucket")
        self.assertEqual(m.region_name, "eu-west-1")
        self.assertIn("HEAD", [method for method, _u, _r in aws.requests])

    def test_a_server_that_names_no_region_is_left_alone(self):
        """A MinIO without a region: no header to read, and no HEAD spent
        asking for one on every bucket open."""
        m = self.aws.attach(make_model(
            endpoint_url="http://minio.local:9000", use_path=True))
        m.enter_bucket("eu-bucket")
        self.assertEqual(m.binding_cache["eu-bucket"],
                         ("http://minio.local:9000", "us-east-1", True))
        self.assertEqual([method for method, _u, _r in self.aws.requests],
                         ["GET"])

    def test_a_redirect_hint_with_the_bucket_host_binds_virtual_host(self):
        """AWS's PermanentRedirect names the bucket's own virtual host. It
        was bound as an endpoint in path style, which addressed
        https://eu-bucket.s3…/eu-bucket — a GET of a key, never a listing."""
        aws = _FakeAws({"eu-bucket": "eu-west-1"}, refuse_global_host=True)
        m = aws.attach(make_model(use_path=True))
        m.enter_bucket("eu-bucket")
        self.assertEqual(
            m.binding_cache["eu-bucket"],
            ("https://s3.eu-west-1.amazonaws.com", "eu-west-1", False))
        self.assertEqual(_credential_region(m.presigned_get_url("k")),
                         "eu-west-1")
        for url in aws.urls():
            self.assertNotRegex(url, r"//eu-bucket\.s3\.[^/]*/eu-bucket")

    def test_a_redirect_from_a_regional_endpoint_signs_for_the_hint(self):
        # The hinted endpoint is tried signed for the region the redirect
        # named, not the one just refused.
        aws = _FakeAws({"eu-bucket": "eu-west-1"})
        m = aws.attach(make_model(
            endpoint_url="https://s3.us-west-2.amazonaws.com",
            region_name="us-west-2"))
        m.enter_bucket("eu-bucket")
        self.assertEqual(
            m.binding_cache["eu-bucket"],
            ("https://s3.eu-west-1.amazonaws.com", "eu-west-1", False))
        method, url, signed = aws.requests[-1]
        self.assertTrue(url.startswith(
            "https://eu-bucket.s3.eu-west-1.amazonaws.com/"))
        self.assertEqual(signed, "eu-west-1")

    def test_bucket_hints_prefer_the_header_to_the_clients_region(self):
        """The HEAD succeeds only because botocore redirected it, so the
        client's own region is the one answer that is certainly wrong."""
        region, _endpoint = self.m.get_bucket_hints("eu-bucket")
        self.assertEqual(region, "eu-west-1")


class RunParallelTests(unittest.TestCase):
    def test_sequential_when_single_worker(self):
        seen = []
        run_parallel([1, 2, 3], seen.append, 1)
        self.assertEqual(seen, [1, 2, 3])   # order preserved

    def test_all_items_processed_in_parallel(self):
        seen = []
        lock = threading.Lock()

        def _fn(item):
            with lock:
                seen.append(item)

        run_parallel(range(50), _fn, 8)
        self.assertEqual(sorted(seen), list(range(50)))

    def test_first_error_is_reraised_and_work_stops(self):
        started = []
        lock = threading.Lock()

        def _fn(item):
            with lock:
                started.append(item)
            raise RuntimeError(f"boom {item}")

        with self.assertRaises(RuntimeError):
            run_parallel(range(20), _fn, 4)
        # the pool drains rather than leaving threads running
        self.assertLessEqual(len(started), 20)

    def test_cancel_event_stops_and_raises(self):
        cancel = threading.Event()
        cancel.set()
        seen = []
        with self.assertRaises(TransferCancelled):
            run_parallel([1, 2, 3], seen.append, 4, cancel_event=cancel)
        self.assertEqual(seen, [])

    def test_cancel_checked_between_sequential_items(self):
        cancel = threading.Event()
        seen = []

        def _fn(item):
            seen.append(item)
            cancel.set()

        with self.assertRaises(TransferCancelled):
            run_parallel([1, 2, 3], _fn, 1, cancel_event=cancel)
        self.assertEqual(seen, [1])

    def test_empty_input(self):
        run_parallel([], lambda _i: None, 4)  # must not raise
class ParallelFilesSettingTests(unittest.TestCase):
    def test_default_and_clamping(self):
        m = make_model()
        self.assertEqual(m.parallel_files, Model.DEFAULT_PARALLEL_FILES)
        self.assertEqual(m.set_parallel_files(0), 1)
        self.assertEqual(m.set_parallel_files(999), Model.MAX_PARALLEL_FILES)
        self.assertEqual(m.set_parallel_files("junk"),
                         Model.DEFAULT_PARALLEL_FILES)

    def test_clone_inherits(self):
        m = make_model(parallel_files=7)
        self.assertEqual(m.clone_for_worker().parallel_files, 7)

    def test_prefix_download_runs_in_parallel(self):
        """A whole-folder download is a single job entry, so the fan-out has
        to happen inside download_file or the setting does nothing for it."""
        m = make_model(bucket="b", parallel_files=4)
        keys = [{"Key": f"dir/f{i}.txt", "Size": 1} for i in range(12)]
        c = FakeS3Client(list_pages=[{"Contents": keys}])
        m._client = c
        with tempfile.TemporaryDirectory() as tmp:
            m.download_file("dir/", None, tmp)
        self.assertEqual(len(c.calls_of("download_file")), 12)

    def test_prefix_copy_runs_in_parallel(self):
        m = make_model(bucket="b", parallel_files=4)
        keys = [{"Key": f"src/f{i}.txt", "Size": 1} for i in range(10)]
        c = FakeS3Client(list_pages=[{"Contents": keys}])
        m._client = c
        m.copy_prefix("src/", "dst/")
        copied = {kw["Key"] for kw in c.calls_of("copy_object")}
        self.assertEqual(copied, {f"dst/f{i}.txt" for i in range(10)})
class ReadOnlyProfileTests(unittest.TestCase):
    """A read-only profile must refuse every write at the model layer, not
    just hide buttons."""

    def setUp(self):
        self.m = make_model(bucket="b", read_only=True)
        self.m._client = FakeS3Client(
            list_pages=[{"Contents": [{"Key": "a.txt", "Size": 1}]}],
            versions_pages=[{}], mpu_pages=[{}],
        )

    def test_writes_are_refused(self):
        cases = {
            "delete": lambda: self.m.delete("a.txt"),
            "delete prefix": lambda: self.m.delete("dir/"),
            "create_folder": lambda: self.m.create_folder("x/"),
            "upload": lambda: self.m.upload_file(None, "x"),
            "copy_object": lambda: self.m.copy_object("a", "b"),
            "copy_prefix": lambda: self.m.copy_prefix("a/", "b/"),
            "create_bucket": lambda: self.m.create_bucket("nb"),
            "delete_bucket": lambda: self.m.delete_bucket("b"),
            "delete_bucket_recursive": lambda: self.m.delete_bucket_recursive("b"),
            "empty_bucket": lambda: self.m.empty_bucket("b"),
            "versioning": lambda: self.m.set_bucket_versioning("Enabled"),
            "storage class": lambda: self.m.change_storage_class("a", "GLACIER"),
            "metadata": lambda: self.m.set_object_metadata("a"),
            "tags": lambda: self.m.put_object_tags("a", []),
            "make public": lambda: self.m.make_object_public("a"),
            "delete version": lambda: self.m.delete_object_version("a", "v1"),
            "make current": lambda: self.m.make_version_current("a", "v1"),
            "abort upload": lambda: self.m.abort_multipart_upload("a", "u1"),
            "presigned put": lambda: self.m.presigned_put_url("a"),
            "undelete": lambda: self.m.undelete("a.txt"),
            "restore": lambda: self.m.restore_object("a"),
        }
        for label, call in cases.items():
            with self.subTest(operation=label):
                with self.assertRaises(ReadOnlyError):
                    call()

    def test_reads_still_work(self):
        self.assertEqual(
            [k for k, _s in self.m.get_keys("")], ["a.txt"])
        self.assertTrue(self.m.presigned_get_url("a.txt"))

    def test_writable_profile_is_unaffected(self):
        m = make_model(bucket="b")
        m._client = FakeS3Client()
        m.create_folder("x/")  # must not raise
        self.assertEqual(len(m._client.calls_of("put_object")), 1)

    def test_clone_keeps_the_flag(self):
        self.assertTrue(self.m.clone_for_worker().read_only)
class RateLimiterTests(unittest.TestCase):
    """Deterministic: the limiter takes injectable clock and sleep."""

    def _limiter(self, rate, **kw):
        self.now = 0.0
        self.slept = []

        def _time():
            return self.now

        def _sleep(seconds):
            self.slept.append(seconds)
            self.now += seconds

        return RateLimiter(rate, time_fn=_time, sleep_fn=_sleep, **kw)

    def test_zero_rate_never_blocks(self):
        limiter = self._limiter(0)
        self.assertEqual(limiter.consume(10 ** 9), 0.0)
        self.assertEqual(self.slept, [])

    def test_burst_within_capacity_is_free(self):
        limiter = self._limiter(1000)          # 1 s of burst = 1000 bytes
        self.assertEqual(limiter.consume(1000), 0.0)
        self.assertEqual(self.slept, [])

    def test_exceeding_the_bucket_sleeps_the_difference(self):
        limiter = self._limiter(1000)
        limiter.consume(1000)                  # drains the bucket
        delay = limiter.consume(500)           # needs another half second
        self.assertAlmostEqual(delay, 0.5, places=6)
        self.assertEqual(len(self.slept), 1)

    def test_tokens_refill_over_time(self):
        limiter = self._limiter(1000)
        limiter.consume(1000)
        self.now += 2.0                        # plenty of time passes
        self.assertEqual(limiter.consume(1000), 0.0)

    def test_sustained_rate_is_respected(self):
        limiter = self._limiter(1000, capacity=0)
        for _ in range(4):
            limiter.consume(500)
        # 2000 bytes at 1000 B/s with no burst allowance ≈ 2 s of waiting
        self.assertAlmostEqual(sum(self.slept), 2.0, places=6)

    def test_negative_and_zero_amounts_ignored(self):
        limiter = self._limiter(1000)
        self.assertEqual(limiter.consume(0), 0.0)
        self.assertEqual(limiter.consume(-5), 0.0)

    def test_model_wires_the_limiter_and_shares_it_with_clones(self):
        m = make_model()
        self.assertIsNone(m.rate_limiter)
        m.set_rate_limit(2048)
        self.assertIsNotNone(m.rate_limiter)
        self.assertIs(m.clone_for_worker().rate_limiter, m.rate_limiter)
        m.set_rate_limit(0)
        self.assertIsNone(m.rate_limiter)

    def test_progress_adapter_charges_the_limiter(self):
        limiter = self._limiter(1000)
        seen = []
        adapter = model_module._BotoProgressAdapter(
            100, "k", lambda t, c, k: seen.append(c), limiter=limiter)
        adapter(60)
        adapter(40)
        self.assertEqual(seen, [60, 100])
        # everything transferred was charged against the bucket
        self.assertAlmostEqual(limiter._tokens, 900.0, places=6)
class RedirectConflictTests(unittest.TestCase):
    def setUp(self):
        self.m = make_model()

    def test_service_endpoint_is_not_a_conflict(self):
        """REGRESSION: service-level redirect hints such as
        's3.us-west-2.amazonaws.com' were rejected as "bound to bucket 's3'"
        (the old check compared the leftmost label against itself), which
        broke PermanentRedirect recovery."""
        self.assertFalse(self.m._redirect_conflicts_with_bucket(
            "https://s3.us-west-2.amazonaws.com", "mybucket"))

    def test_same_bucket_endpoint_is_not_a_conflict(self):
        self.assertFalse(self.m._redirect_conflicts_with_bucket(
            "https://mybucket.s3.us-west-2.amazonaws.com", "mybucket"))

    def test_other_bucket_endpoint_conflicts(self):
        self.assertTrue(self.m._redirect_conflicts_with_bucket(
            "https://other.s3.us-west-2.amazonaws.com", "mybucket"))
        # legacy dash-style regional host
        self.assertTrue(self.m._redirect_conflicts_with_bucket(
            "https://other.s3-us-west-2.amazonaws.com", "mybucket"))

    def test_non_aws_hosts_are_not_conflicts(self):
        self.assertFalse(self.m._redirect_conflicts_with_bucket(
            "https://minio.company.local", "mybucket"))
        self.assertFalse(self.m._redirect_conflicts_with_bucket(
            "https://localhost:9000", "mybucket"))
class TransferConcurrencyTests(unittest.TestCase):
    def test_default_applied_to_both_configs(self):
        m = make_model()
        self.assertEqual(m.transfer_concurrency,
                         Model.DEFAULT_TRANSFER_CONCURRENCY)
        self.assertEqual(m.transfer_cfg_download.max_concurrency,
                         m.transfer_concurrency)
        self.assertEqual(m.transfer_cfg_upload.max_concurrency,
                         m.transfer_concurrency)

    def test_explicit_value_and_clamping(self):
        m = make_model(transfer_concurrency=8)
        self.assertEqual(m.transfer_concurrency, 8)
        self.assertEqual(m.transfer_cfg_download.max_concurrency, 8)
        self.assertEqual(m.set_transfer_concurrency(0), 1)
        self.assertEqual(m.set_transfer_concurrency(999),
                         Model.MAX_TRANSFER_CONCURRENCY)
        self.assertEqual(m.set_transfer_concurrency("garbage"),
                         Model.DEFAULT_TRANSFER_CONCURRENCY)
        self.assertEqual(m.transfer_cfg_upload.max_concurrency,
                         Model.DEFAULT_TRANSFER_CONCURRENCY)

    def test_clone_inherits_concurrency(self):
        m = make_model(transfer_concurrency=6)
        c = m.clone_for_worker()
        self.assertEqual(c.transfer_concurrency, 6)
        self.assertEqual(c.transfer_cfg_download.max_concurrency, 6)
class PublicBaseUrlTests(unittest.TestCase):
    """A bucket served through CloudFront or a custom domain: the endpoint
    URL is correct and useless."""

    def test_the_base_url_replaces_the_endpoint(self):
        m = make_model(bucket="assets", public_base_url="https://cdn.example.com/")
        self.assertEqual(m.direct_object_url("img/logo.png"),
                         "https://cdn.example.com/img/logo.png")

    def test_the_bucket_is_not_repeated_in_the_cdn_path(self):
        """The domain is mapped at the bucket root, so the bucket name is not
        part of the path there."""
        m = make_model(bucket="assets", public_base_url="https://cdn.example.com")
        self.assertNotIn("assets", m.direct_object_url("a.png"))

    def test_without_a_base_url_nothing_changes(self):
        m = make_model(bucket="assets")
        self.assertEqual(m.direct_object_url("a.png"),
                         "https://s3.amazonaws.com/assets/a.png")

    def test_presigned_urls_are_left_alone(self):
        """Their signature covers the host, so rewriting one breaks it."""
        m = make_model(bucket="assets", public_base_url="https://cdn.example.com")
        m._client = FakeS3Client()
        self.assertNotIn("cdn.example.com", m.presigned_get_url("a.png", 60))

    def test_the_clone_carries_it(self):
        m = make_model(bucket="assets", public_base_url="https://cdn.example.com")
        self.assertEqual(m.clone_for_worker().public_base_url,
                         "https://cdn.example.com")

    def test_a_hash_in_the_key_does_not_become_a_fragment(self):
        """"notes#2.txt" pasted raw addresses the object "notes"."""
        m = make_model(bucket="assets", public_base_url="https://cdn.example.com")
        self.assertEqual(m.direct_object_url("notes#2.txt"),
                         "https://cdn.example.com/notes%232.txt")

    def test_a_question_mark_does_not_become_a_query(self):
        m = make_model(bucket="assets", public_base_url="https://cdn.example.com")
        self.assertEqual(m.direct_object_url("what?.txt"),
                         "https://cdn.example.com/what%3F.txt")

    def test_spaces_and_percent_signs_are_encoded(self):
        m = make_model(bucket="assets", public_base_url="https://cdn.example.com")
        self.assertEqual(m.direct_object_url("a b/100% done.txt"),
                         "https://cdn.example.com/a%20b/100%25%20done.txt")

    def test_slashes_stay_slashes(self):
        """They are the path separators the object hierarchy is built from."""
        m = make_model(bucket="assets", public_base_url="https://cdn.example.com")
        self.assertEqual(m.direct_object_url("a/b/c.png"),
                         "https://cdn.example.com/a/b/c.png")

    def test_the_endpoint_form_is_encoded_the_same_way(self):
        m = make_model(bucket="assets")
        self.assertEqual(m.direct_object_url("notes#2.txt"),
                         "https://s3.amazonaws.com/assets/notes%232.txt")

    def test_an_ordinary_key_is_untouched(self):
        m = make_model(bucket="assets", public_base_url="https://cdn.example.com")
        self.assertEqual(m.direct_object_url("img/logo-1.0_final~2.png"),
                         "https://cdn.example.com/img/logo-1.0_final~2.png")

    def test_the_direct_url_encodes_a_key_exactly_as_botocore_does(self):
        """The two links have to address the same object: a presigned URL is
        built by botocore, a direct one by us, and users compare them."""
        key = "notes#2 (final)~x/a b%c.txt"
        m = make_model(bucket="assets", use_path=True)
        signed = m.client.generate_presigned_url(
            "get_object", Params={"Bucket": "assets", "Key": key}, ExpiresIn=60)
        self.assertEqual(m.direct_object_url(key), signed.split("?")[0])
class RequesterPaysTests(unittest.TestCase):
    """A requester-pays bucket refuses every read that does not opt in."""

    def _emit(self, client, operation):
        params = {}
        client.meta.events.emit(
            f"provide-client-params.s3.{operation}",
            params=params, model=None, context={})
        return params

    def test_object_operations_opt_in(self):
        m = make_model(bucket="b", requester_pays=True)
        client = m.client
        for operation in ("GetObject", "HeadObject", "ListObjectsV2",
                          "UploadPart", "DeleteObjects", "RestoreObject"):
            with self.subTest(operation=operation):
                self.assertEqual(self._emit(client, operation),
                                 {"RequestPayer": "requester"})

    def test_bucket_operations_are_left_alone(self):
        """They reject the parameter outright — it is a ParamValidationError,
        not something the service ignores."""
        m = make_model(bucket="b", requester_pays=True)
        client = m.client
        for operation in ("ListBuckets", "GetBucketVersioning",
                          "CreateBucket", "GetBucketLifecycleConfiguration"):
            with self.subTest(operation=operation):
                self.assertEqual(self._emit(client, operation), {})

    def test_nothing_is_added_when_the_profile_is_off(self):
        m = make_model(bucket="b")
        self.assertEqual(self._emit(m.client, "GetObject"), {})

    def test_an_explicit_payer_is_not_overwritten(self):
        m = make_model(bucket="b", requester_pays=True)
        params = {"RequestPayer": "bucketowner"}
        m.client.meta.events.emit(
            "provide-client-params.s3.GetObject",
            params=params, model=None, context={})
        self.assertEqual(params["RequestPayer"], "bucketowner")

    def test_a_worker_clone_keeps_the_setting(self):
        m = make_model(bucket="b", requester_pays=True)
        clone = m.clone_for_worker()
        self.assertTrue(clone.requester_pays)
        self.assertEqual(self._emit(clone.client, "GetObject"),
                         {"RequestPayer": "requester"})

    def test_every_listed_operation_really_accepts_the_parameter(self):
        """The hook injects RequestPayer unconditionally, so an operation
        listed here that does not take it turns into a ParamValidationError
        the moment a requester-pays profile reaches it. DeleteObjectTagging
        was on the list and is one of those — it takes Bucket, Key, VersionId
        and ExpectedBucketOwner only."""
        service = make_model(bucket="b").client.meta.service_model
        for operation in Model.REQUESTER_PAYS_OPERATIONS:
            with self.subTest(operation=operation):
                shape = service.operation_model(operation).input_shape
                self.assertIn("RequestPayer", (shape.members if shape else {}))

    def test_delete_object_tagging_is_not_on_the_list(self):
        self.assertNotIn("DeleteObjectTagging",
                         Model.REQUESTER_PAYS_OPERATIONS)
class RedactUrlCredentialsTests(unittest.TestCase):
    """Everything that can appear in a proxy field, sanitised."""

    def test_nothing_to_redact_is_left_exactly_alone(self):
        for url in ("http://proxy.corp:3128", "proxy.corp:3128",
                    "https://proxy.corp", "http://[2001:db8::1]:3128"):
            with self.subTest(url=url):
                self.assertEqual(
                    diagnostics.redact_url_credentials(url), url)

    def test_empty_values_stay_empty(self):
        for value in ("", None, "   "):
            self.assertEqual(diagnostics.redact_url_credentials(value), "")

    def test_a_password_is_replaced(self):
        self.assertEqual(
            diagnostics.redact_url_credentials(
                "http://alice:s3cr3t@proxy.corp:3128"),
            "http://***@proxy.corp:3128")

    def test_a_bare_username_goes_too(self):
        self.assertEqual(
            diagnostics.redact_url_credentials("http://alice@proxy.corp"),
            "http://***@proxy.corp")

    def test_credentials_without_a_scheme_are_still_caught(self):
        self.assertEqual(
            diagnostics.redact_url_credentials("alice:s3cr3t@proxy.corp:3128"),
            "***@proxy.corp:3128")

    def test_an_at_sign_inside_the_password_does_not_shift_the_host(self):
        self.assertEqual(
            diagnostics.redact_url_credentials(
                "http://alice:p@ss@proxy.corp:3128"),
            "http://***@proxy.corp:3128")

    def test_an_ipv6_host_survives_intact(self):
        self.assertEqual(
            diagnostics.redact_url_credentials(
                "http://alice:pw@[2001:db8::1]:3128"),
            "http://***@[2001:db8::1]:3128")

    def test_an_at_sign_in_the_path_is_not_mistaken_for_credentials(self):
        for url in ("http://proxy.corp/go@home", "http://proxy.corp?to=a@b",
                    "http://proxy.corp#a@b"):
            with self.subTest(url=url):
                self.assertEqual(
                    diagnostics.redact_url_credentials(url), url)

    def test_an_unencoded_delimiter_in_the_password_is_still_caught(self):
        """The proxy that will not parse is exactly the one a user files a
        report about: "/", "?" or "#" in the password ended the authority
        early and the rest of the password went out in clear."""
        for url in ("http://alice:p/ss@proxy.corp:3128",
                    "http://alice:p?ss@proxy.corp:3128",
                    "http://alice:p#ss@proxy.corp:3128",
                    "http://alice:/ss@proxy.corp:3128"):
            with self.subTest(url=url):
                self.assertEqual(diagnostics.redact_url_credentials(url),
                                 "http://***@proxy.corp:3128")

    def test_an_at_sign_after_a_real_port_is_still_a_path(self):
        self.assertEqual(
            diagnostics.redact_url_credentials(
                "http://proxy.corp:3128/go@home"),
            "http://proxy.corp:3128/go@home")

    def test_the_path_is_kept_when_there_are_credentials(self):
        self.assertEqual(
            diagnostics.redact_url_credentials("http://a:b@proxy.corp/pac?x=1"),
            "http://***@proxy.corp/pac?x=1")

    def test_a_malformed_value_leaks_nothing(self):
        """Half a URL is still half a password."""
        for url in ("http://alice:s3cr3t@", "alice:s3cr3t@", "@", "://"):
            with self.subTest(url=url):
                self.assertNotIn(
                    "s3cr3t", diagnostics.redact_url_credentials(url))
class ProxyAndCaBundleTests(unittest.TestCase):
    """The only TLS escape hatch used to be turning verification off; a
    private CA should be trusted by path instead."""

    @classmethod
    def setUpClass(cls):
        # The diagnostics report draws icons, which needs an application;
        # without one this module aborted when run on its own.
        cls._app = _ensure_qapp()

    def test_no_proxy_by_default(self):
        self.assertEqual(make_model().proxy_settings(), {})

    def test_a_bare_host_gets_a_scheme(self):
        m = make_model(proxy_url="proxy.corp:3128")
        self.assertEqual(m.proxy_settings(),
                         {"http": "http://proxy.corp:3128",
                          "https": "http://proxy.corp:3128"})

    def test_an_explicit_scheme_is_kept(self):
        m = make_model(proxy_url="https://proxy.corp:8443")
        self.assertEqual(m.proxy_settings()["https"],
                         "https://proxy.corp:8443")

    def test_the_proxy_reaches_the_botocore_config(self):
        m = make_model(proxy_url="http://proxy.corp:3128")
        self.assertEqual(m.client.meta.config.proxies,
                         {"http": "http://proxy.corp:3128",
                          "https": "http://proxy.corp:3128"})

    def test_a_ca_bundle_is_a_path_not_a_boolean(self):
        """Trusting one extra CA is not the same as trusting anything that
        answers."""
        with tempfile.NamedTemporaryFile(suffix=".pem") as bundle:
            m = make_model(ca_bundle=bundle.name)
            self.assertEqual(m.check_tls_settings(), "")
            # botocore stores it on the endpoint's verify setting
            self.assertEqual(m.client.meta.endpoint_url,
                             "https://s3.amazonaws.com")

    def test_a_missing_bundle_is_reported_before_it_becomes_an_sslerror(self):
        m = make_model(ca_bundle="/nonexistent/ca.pem")
        self.assertIn("not found", m.check_tls_settings())

    def test_switching_verification_off_makes_the_bundle_meaningless(self):
        with tempfile.NamedTemporaryFile(suffix=".pem") as bundle:
            m = make_model(ca_bundle=bundle.name, no_ssl_check=True)
            self.assertIn("ignored", m.check_tls_settings())

    def test_a_clean_profile_has_nothing_to_report(self):
        self.assertEqual(make_model().check_tls_settings(), "")

    def test_a_worker_clone_keeps_both(self):
        m = make_model(proxy_url="http://p:1", ca_bundle="/tmp/ca.pem")
        clone = m.clone_for_worker()
        self.assertEqual(clone.proxy_url, "http://p:1")
        self.assertEqual(clone.ca_bundle, "/tmp/ca.pem")

    def test_diagnostics_names_both(self):
        m = make_model(proxy_url="http://p:1", ca_bundle="/tmp/ca.pem")
        text = diagnostics.format_report(diagnostics.collect(
            PROJECT_ROOT,
            version="9.9.9", model=m))
        self.assertIn("/tmp/ca.pem", text)
        self.assertIn("http://p:1", text)

    def test_diagnostics_says_direct_when_unset(self):
        text = diagnostics.format_report(diagnostics.collect(
            PROJECT_ROOT,
            version="9.9.9", model=make_model()))
        self.assertIn("(direct)", text)
        self.assertIn("(system trust store)", text)
