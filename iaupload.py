# Some code for uploading very large files to the Internet Archive
# https://archive.org/help/abouts3.txt

# Haven't tested on py2 at all, but maybe this will improve your changes :-)
from __future__ import (
    division, print_function, absolute_import)

import os
import os.path
import socket
import sys
import time
import threading

import boto3
from boto3.s3.transfer import S3Transfer, TransferConfig
from botocore.config import Config

import requests

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


# The boto3 needs-retry handlers are super undocumented!  It looks like the
# way it works is, after a response, all the needs-retry.<service>.<operation>
# handlers are called in order, and they can either return None or else return
# a float; if they return a float, then botocore will sleep that long and then
# retry the operation. See botocore/endpoint.py
def retry_on_slowdown_handler(response, attempts, **kwargs):
    # Try to get some more information on the weird cascading failures I've
    # seen occasionally, with a bunch of chained connection dropped exceptions
    if (response is None
        or response[1]["ResponseMetadata"]["HTTPStatusCode"] != 200):
        print("Saw unusual response in retry handler: {!r}"
              .format(response))

    # Copied from botocore.handlers.check_for_200_error:
    if response is None:
        # A None response can happen if an exception is raised while
        # trying to retrieve the response.  See Endpoint._get_response().
        return
    _, parsed = response
    if (parsed["ResponseMetadata"]["HTTPStatusCode"] == 503
        and parsed["Error"]["Code"] == "SlowDown"):
        # Sleep 5 seconds first time, then 10, 20, 40, 80... with a max of 5
        # minutes between attempts
        sleep_time = min(5 * 2 ** attempts, 5 * 60)
        print("Got SlowDown (on attempt {}), sleeping {} sec"
              .format(attempt, sleep_time))
        return sleep_time
    else:
        # No problem, carry on
        return None


def ia_client(extra_headers):
    s = boto3.Session(
        aws_access_key_id=os.environ["IA_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["IA_SECRET_ACCESS_KEY"],
        )

    if extra_headers:
        extra_headers_handler = make_extra_headers_handler(extra_headers)
        for operation in ["CreateMultipartUpload", "CompleteMultipartUpload",
                          "UploadPart", "PutObject",
                          # this can get SlowDown errors, which is very
                          # annoying
                          "AbortMultipardUpload",
                         ]:
            s.events.register("before-call.s3.{}".format(operation),
                              extra_headers_handler)
            s.events.register("needs-retry.s3.{}".format(operation),
                              retry_on_slowdown_handler)

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
        self._lock = threading.Lock()

    # Thread-safe API, combining ready() + reset() into a single atomic op
    # Needed because boto3.s3.transfer likes to call progress callbacks from
    # random threads
    def acquire(self):
        with self._lock:
            if self.ready():
                self.reset()
                return True
            else:
                return False

    # These two operations are not thread-safe
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
        if not force and not self._rate_limiter.acquire():
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

    def transfer_callback(self, bytes):
        self._transferred += bytes
        self.redraw()


def current_remote_size(ia_item_name, remote_filename):
    # Supposedly you can get this via the S3 API, but that was *incredibly*
    # slow when I tried on a large file... like boto3 just times out after
    # multiple minutes. Maybe it does something silly like computing the hash
    # of the file for the ETag header?
    url = "http://archive.org/download/{}/{}".format(
        ia_item_name, remote_filename)
    response = requests.head(url, allow_redirects=True)
    if response.status_code == 404:
        return None
    elif response.status_code == 200:
        return int(response.headers["Content-Length"])
    else:
        response.raise_for_status()
        assert False


def accesskey_tasks_queued():
    # This is using an API that isn't supposed to be public, "may change at
    # any time" according to the docs, and probably isn't even giving us
    # exactly the right thing (we really want to know how many tasks are
    # queued for the bucket we're uploading to, not for the current
    # accesskey). But I can't figure out a better way to do it...
    r = requests.get("http://s3.us.archive.org/?check_limit=1&accesskey={}"
                     .format(os.environ["IA_ACCESS_KEY_ID"]))
    #print("  debug: {!r}".format(r.json()))
    return r.json()["detail"]["accesskey_tasks_queued"]


def wait_for_queue_drain(ia_item_name):
    # We ignore ia_item_name for now; see comments in accesskey_tasks_queued
    # :-(
    start = time.time()
    pause = 1
    while accesskey_tasks_queued():
        print("...waiting for task queue to drain (waited {:.1f} s, "
              "will check again in {} s)"
              .format(time.time() - start, pause))
        time.sleep(pause)
        # exponential backoff with 10 minute max
        pause = min(pause * 2, 10 * 60)


# Returns bytes uploaded for stat tracking purposes,
# or None if upload was skipped
def ia_upload(local_filename, ia_item_name, remote_filename,
              # 1 gigabyte
              min_chunksize=10 ** 9,
              # Empirically we can start getting into task queue overflow at
              # around 500-900 tasks, so we avoid queueing more than ~200 at a
              # time.
              max_chunks=200,
              max_concurrency=10,
              extra_headers=DEFAULT_HEADERS,
              show_progress=True,
              skip_if_present=True):
    print("Uploading {} -> {} / {}".format(
        local_filename, ia_item_name, remote_filename))

    # Wait for the IA task queue to drain before starting an upload.
    # We can upload a file *much* faster than IA's internal code can process
    # it -- and the internal processing is done in a totally serial
    # fashion. And if the internal processing queue gets too large, then bad
    # things start happening -- requests return SlowDown errors (even
    # AbortMultipartUpload requests -- it's too busy to quit! also
    # AbortMultipartUpload doesn't actually stop the slow processing that's
    # already queued up...), and eventually boto3 blows up for reasons that
    # aren't entirely clear (multiple chained "connection dropped, and while
    # processing that, connection dropped, and while processing that..."
    # tracebacks). So we try to pre-emptively apply our own backpressure
    # before we enter that overload regime.
    #
    # Also, it's important to do this *before* checking whether a file is
    # already present, because it might have been uploaded but not be visible
    # until the pending queue tasks have finished.
    #
    # To see the queue for a given item (= bucket):
    #     https://catalogd.archive.org/history/$BUCKETNAME
    wait_for_queue_drain(ia_item_name)

    local_size = os.stat(local_filename).st_size
    if skip_if_present:
        remote_size = current_remote_size(ia_item_name, remote_filename)
        if remote_size is not None:
            if remote_size == local_size:
                if show_progress:
                    print("{}: already uploaded; skipping"
                          .format(local_filename))
            else:
                print("{}: already uploaded; but watch out -- "
                      "sizes don't match! (local={}, remote={}) -- "
                      "skipping anyway"
                      .format(local_filename, local_size, remote_size))
            # Didn't upload anything
            return None

    # We put an upper bound on how many chunks we use for a single item,
    # because internally archive.org uses a weird system where each UploadPart
    # queues a task on a job queue, uploads start returning SlowDown errors
    # once the queue gets big enough... and the queue doesn't empty at all
    # while uploads are happening (because of a tricky thing where the first
    # task in the queue greedily starts doing the work of all the later tasks,
    # but the queue manager can't see this). So if we want an upload to
    # complete, it's a good idea to not use too many chunks.
    # ...of course this doesn't make much difference if we are doing multiple
    # uploads in a row to the same item (since the queue is per-item), but
    # what can you do...
    chunksize = max(int(local_size / max_chunks) + 1, min_chunksize)
    transfer = ia_transfer(chunksize, max_concurrency, extra_headers)
    if show_progress:
        progress_bar = UploadProgressBar(local_filename)
    transfer.upload_file(local_filename, ia_item_name, remote_filename,
                         callback=progress_bar.transfer_callback)
    progress_bar.redraw(force=True)
    # Bytes uploaded
    return local_size


def expand_filename(local_filename):
    if os.path.isdir(local_filename):
        for root, _, leaf_filenames in os.walk(local_filename):
            for leaf_filename in leaf_filenames:
                yield os.path.normpath(os.path.join(root, leaf_filename))
    else:
        yield local_filename


def ia_upload_all(filenames_or_dirs, ia_item_name, **kwargs):
    start = time.time()
    total_uploaded_bytes = 0
    total_uploaded_files = 0
    if isinstance(filenames_or_dirs, str):
        filenames_or_dirs = [filenames_or_dirs]
    for filename_or_dir in filenames_or_dirs:
        for filename in expand_filename(filename_or_dir):
            upload_result = ia_upload(filename,
                                      ia_item_name,
                                      # normpath converts "./foo" into "foo"
                                      os.path.normpath(filename),
                                      **kwargs)
            if upload_result is not None:
                total_uploaded_bytes += upload_result
                total_uploaded_files += 1
            now = time.time()
            print("SUMMARY SO FAR: uploaded {} files ({:.1f} GB) in {:.1f} s "
                  "(effective average transfer rate: {:.2f} MB/s)"
                  .format(total_uploaded_files,
                          total_uploaded_bytes / 10**9,
                          now - start,
                          total_uploaded_bytes / 10**6 / (now - start),
                          ))
            print()
#