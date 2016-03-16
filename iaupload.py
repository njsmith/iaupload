# Some code for uploading very large files to the Internet Archive

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

# Only execute this debugging thing one pers session
if "_already_loaded" not in globals():
    boto3.set_stream_logger(name="botocore")
    _already_loaded = True

# def tell_me_more(*args, **kwargs):
#     print(args, kwargs)

def make_add_headers_handler(extra_headers):
    # This callback will get run just before an actual request is made (if
    # it's registered as a handler for the appropriate "before-call" event),
    # and has a chance to mess with the request before it happens.
    def add_headers_handler(**kwargs):
        kwargs["params"]["headers"].update(extra_headers)
        # In the botocore event system, 'None' means 'continue processing'
        return None
    return add_headers_handler

def ia_client(extra_headers):
    s = boto3.Session(
        aws_access_key_id=os.environ["IA_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["IA_SECRET_ACCESS_KEY"],
        )

    if extra_headers:
        handler = make_add_headers_handler(extra_headers)
        s.events.register("before-call.s3.CreateMultipartUpload", handler)
        s.events.register("before-call.s3.CompleteMultipartUpload", handler)

    c = s.client("s3",
                 # Explicit http:// here disables TLS
                 endpoint_url="http://s3.us.archive.org",
                 # Disable host-based addressing (equivalent of
                 # calling_format=OrdinaryCallingFormat() in boto 2)
                 config=Config(s3={"addressing_style": "path"}),
                 )
    return c


# Other things to try:
#   "x-archive-simulate-error": "SlowDown"
#   "x-archive-auto-make-bucket": "1"
DEFAULT_HEADERS = {
    "x-archive-queue-derive": "0",
}

def ia_transfer(chunksize, max_concurrency, extra_headers):
    # We always go through the multipart upload path, to make things simpler
    # (e.g. this way we only have to worry about overriding the HTTP headers
    # for multipart uploads, not regular uploads)
    config = TransferConfig(multipart_threshold=0,
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

        percent = 100.0 * self._transferred / self._size
        MB = self._transferred / 1e6
        duration = time.time() - self._start
        rate = MB / duration
        self._out.write("{local_filename}: {percent:.2f}%, {MB:.1f} MB in "
                        "{duration:.1f} s, {rate:.1f} MB/s\n"
                        .format(local_filename=self._local_filename,
                                percent=percent,
                                MB=MB,
                                duration=duration,
                                rate=rate))

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

# loop over files, pick target name, start multipart upload
# track pieces in flight and when each upload finishes mark it complete; abort
# all partial uploads if die

# from s3.transfer import ReadFileChunk
# # will automatically trim chunk_size if it goes past end of file
# # and will automatically propagate the actual size through so we don't have to
# # set ContentLength manually.
# # This is a context manager, naturally
# f = ReadFileChunk.from_filename(filename, start_byte, chunk_size,
#                                 callback=foo, enable_callback=True)
# # will call foo(bytes_read) whenever bytes are read
# # note that bytes_read can be negative e.g. if got redirected and have to
# # start uploading again

# client.create_multipart_upload
# r = client.upload_part(Bucket, Key, UploadId, PartNumber, Body, **extra_args)
# etag = r["ETag"]

# Body can be a "seekable file-like object",
# ContentLength can be specified
# ContentMD5 -- don't think we care
#

# client.complete_multipart_upload(
#     Bucket, Key, UploadId,
#     MultipartUpload={"Parts":
#                      [{"ETag": ..., "PartNumber": ...},
#                       ...]})
# client.abort_multipart_upload(...)

# def upload_part(upload_id, filename, key, callback, extra

# class MultipartUploadTracker(object):
#     def __init__(self):
#         self._in_flight = []




#