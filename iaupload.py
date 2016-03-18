# Some code for uploading very large files to the Internet Archive

# Haven't tested on py2 at all, but maybe this will improve your changes :-)
from __future__ import (
    division, print_function, absolute_import)

import os
import os.path
import socket
import sys
import time

import boto3
from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore.config import Config

if not set(os.environ).issuperset(["IA_ACCESS_KEY_ID",
                                   "IA_SECRET_ACCESS_KEY"]):
    raise RuntimeError("Please set IA_ACCESS_KEY_ID and "
                       "IA_SECRET_ACCESS_KEY envvars")

# Only execute this debugging thing one per session
# if "_already_loaded" not in globals():
#     boto3.set_stream_logger(name="botocore")
#     _already_loaded = True


def make_extra_headers_handler(extra_headers):
    # This callback will get run just before an actual request is made (if
    # it's registered as a handler for the appropriate "before-call" event),
    # and has a chance to mess with the request before it happens.
    def extra_headers_handler(**kwargs):
        kwargs["params"]["headers"].update(extra_headers)
        # In the botocore event system, 'None' means 'continue processing'
        return None
    return extra_headers_handler


def ia_client(extra_headers):
    s = boto3.Session(
        aws_access_key_id=os.environ["IA_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["IA_SECRET_ACCESS_KEY"],
        )

    if extra_headers:
        handler = make_extra_headers_handler(extra_headers)
        for operation in ["CreateMultipartUpload", "CompleteMultipartUpload",
                          "UploadPart", "PutObject"]:
            s.events.register("before-call.s3.{}".format(operation), handler)

    c = s.client("s3",
                 # Explicit http:// here disables TLS
                 endpoint_url="http://s3.us.archive.org",
                 # Disable host-based addressing (equivalent of
                 # calling_format=OrdinaryCallingFormat() in boto 2)
                 # XX FIXME: poking at <item>.s3.us.archive.org suggests that
                 # archive.org has implemented the default dns-based
                 # addressing style?
                 config=Config(s3={"addressing_style": "path"}),
                 )

    # XX FIXME HACK: monkeypatch boto3 so that it doesn't include an
    # xmlns="..." attribute on the CompleteMultipartUpload payload. As of
    # 2016-03-17, the inclusion of the xmlns="..." breaks the archive.org S3
    # implementation. This has been reported to archive.org and should be
    # fixed soon.
    del (c._service_model._shape_resolver._shape_map
         ["CompleteMultipartUploadRequest"]
         ["members"]
         ["MultipartUpload"]
         ["xmlNamespace"])

    return c


# Other things to try:
#   "x-archive-simulate-error": "SlowDown"
#   "x-archive-auto-make-bucket": "1"
DEFAULT_HEADERS = {
    "x-archive-queue-derive": "0",
}

def ia_transfer(chunksize, max_concurrency, extra_headers):
    config = TransferConfig(multipart_threshold=chunksize,
                            multipart_chunksize=chunksize,
                            max_concurrency=max_concurrency)
    client = ia_client(extra_headers)
    return S3Transfer(client, config)


class RateLimiter(object):
    def __init__(self, min_update_interval_seconds):
        self._min_update_interval_seconds = min_update_interval_seconds
        self._last_update = 0

    def ready(self):
        now = time.time()
        delta = now - self._last_update
        return delta >= self._min_update_interval_seconds

    def reset(self):
        self._last_update = time.time()


class UploadProgressBar(object):
    def __init__(self, local_filename,
                 out=sys.stdout, min_update_interval_seconds=1):
        self._local_filename = local_filename
        self._size = os.stat(self._local_filename).st_size
        self._transferred = 0
        self._out = out
        self._rate_limiter = RateLimiter(min_update_interval_seconds)
        self._start = time.time()

    def redraw(self, force=False):
        if not force and not self._rate_limiter.ready():
            return

        percent = 100 * self._transferred / self._size
        MB = self._transferred / 1e6
        duration = time.time() - self._start
        rate = MB / duration
        remaining_bytes = self._size - self._transferred
        remaining_sec = duration * remaining_bytes / self._transferred
        remaining_min = remaining_sec / 60
        self._out.write("{local_filename}: {percent:.2f}%, {MB:.1f} MB in "
                        "{duration:.1f} s, {rate:.1f} MB/s "
                        "(eta: {remaining_min:.2f} min)\n"
                        .format(local_filename=self._local_filename,
                                percent=percent,
                                MB=MB,
                                duration=duration,
                                rate=rate,
                                remaining_min=remaining_min))

        self._rate_limiter.reset()

    def transfer_callback(self, bytes):
        self._transferred += bytes
        self.redraw()


def ia_upload(local_filename, ia_item_name, remote_filename,
              # 1 gigabyte
              #chunksize=10 ** 9,
              chunksize=10 ** 7,
              max_concurrency=10,
              extra_headers=DEFAULT_HEADERS,
              show_progress=True):
    transfer = ia_transfer(chunksize, max_concurrency, extra_headers)
    if show_progress:
        progress_bar = UploadProgressBar(local_filename)
    transfer.upload_file(local_filename, ia_item_name, remote_filename,
                         callback=progress_bar.transfer_callback)
    progress_bar.redraw(force=True)


def expand_filename(local_filename):
    if os.path.isdir(local_filename):
        for root, _, leaf_filenames in os.walk(local_filename):
            for leaf_filename in leaf_filenames:
                yield os.path.join(root, leaf_filename)
    else:
        yield local_filename


def ia_upload_all(filenames_or_dirs, ia_item_name, **kwargs):
    if isinstance(filenames_or_dirs, str):
        filenames_or_dirs = [filenames_or_dirs]
    for filename_or_dir in filenames_or_dirs:
        for filename in expand_filename(filename_or_dir):
            ia_upload(filename, ia_item_name, filename, **kwargs)
