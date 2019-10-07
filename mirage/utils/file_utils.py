"""
Utility functions for working with files.  All code that accesses MIRAGE_DATA or CRDS
files should use these functions instead of their standard package counterparts, so
that cloud environments can provide access to files from S3.
"""

import sys
import io
import os
from glob import glob as filesystem_glob
import atexit
import fnmatch
import re

import yaml
from astropy.io import fits, ascii
import asdf
import h5py


__all__ = [
    "open",
    "read_fits",
    "read_asdf",
    "read_ascii_table",
    "read_hdf5",
    "read_yaml",
    "exists",
    "abspath",
    "glob",
    "isfile",
    "isdir"
]


_SYSTEM_ENCODING = sys.getdefaultencoding()
_OPEN_MODES = {"r", "rb"}
_S3_CLIENT = None
_GLOB_CHARACTER_RE = re.compile(r"[\*\?\[]")


def open(uri, mode="r", encoding=None):
    """
    Cloud-enabled version of io.open.  Does not at this time fully
    implement the io.open interface.

    Parameters
    ----------
    uri : str
        Filesystem path or S3 URL (s3://bucket-name/some/object/key)
    mode : str, optional
        File mode.  Currently only read modes are supported.
    encoding : str, optional
        Encoding used to interpret file contents in text mode.  Defaults
        to the system default encoding.

    Returns
    -------
    io.BufferedReader, io.TextIOWrapper, or io.BytesIO
        Open filelike object.  The exact class depends on the type of URI
        and mode.
    """

    if not mode in _OPEN_MODES:
            raise ValueError("Non-read modes are not supported")

    uri = os.path.expandvars(uri)

    if _is_s3_uri(uri):
        _ensure_s3_client()

        bucket_name, key = _parse_s3_uri(uri)
        file = _S3_CLIENT.get_object(bucket_name, key)
        if mode == "rb":
            return file
        else:
            if encoding is None:
                encoding = _SYSTEM_ENCODING
            return io.TextIOWrapper(file, encoding=encoding)
    else:
        return io.open(uri, mode=mode, encoding=encoding)


def read_fits(uri, *args, **kwargs):
    """
    Open a FITS file in readonly mode from either S3
    or the filesystem.
    """

    return fits.open(open(uri, "rb"), *args, **kwargs)


def read_asdf(uri, *args, **kwargs):
    """
    Read an ASDF file from either S3 or the filesystem.
    """

    return asdf.open(open(uri, "rb"), *args, **kwargs)


def read_ascii_table(uri, *args, **kwargs):
    """
    Read an astropy ASCII table from either S3 or the filesystem.
    """

    with open(uri) as file:
        return ascii.read(file.read(), *args, **kwargs)


def read_hdf5(uri, mode="r", *args, **kwargs):
    """
    Read an HDF5 file from either S3 or the filesystem.
    """

    return h5py.File(open(uri, "rb"), mode, *args, **kwargs)


def read_yaml(uri, *args, **kwargs):
    """
    Read a YAML file from either S3 or the filesystem.
    """

    with open(uri) as file:
        return yaml.safe_load(file, *args, **kwargs)


def isfile(uri):
    """
    Cloud-enabled version of os.path.isfile.  For S3, checks if
    the specified object exists.
    """

    uri = os.path.expandvars(uri)

    if _is_s3_uri(uri):
        _ensure_s3_client()

        bucket_name, key = _parse_s3_uri(uri)
        return _S3_CLIENT.object_exists(bucket_name, key)
    else:
        return os.path.isfile(uri)


def isdir(uri):
    """
    Cloud-enabled version of os.path.isdir.  In the case of S3,
    checks if the specified key prefix exists (with a trailing
    slash).
    """

    uri = os.path.expandvars(uri)

    if _is_s3_uri(uri):
        _ensure_s3_client()

        bucket_name, key = _parse_s3_uri(uri)
        if not key.endswith("/"):
            key = key + "/"
        return _S3_CLIENT.prefix_exists(bucket_name, key)
    else:
        return os.path.isdir(uri)


def exists(uri):
    """
    Cloud-enabled version of os.path.exists.  In the case of S3,
    checks if isfile or isdir returns True.
    """

    uri = os.path.expandvars(uri)

    if _is_s3_uri(uri):
        return isdir(uri) or isfile(uri)
    else:
        return os.path.exists(uri)


def abspath(uri):
    """
    Cloud-enabled version of os.path.abspath.  In the case of S3,
    returns the unmodified URI.
    """

    uri = os.path.expandvars(uri)

    if _is_s3_uri(uri):
        return uri
    else:
        return os.path.abspath(uri)


def glob(uri):
    """
    Cloud-enabled version of glob.glob.  In the case of S3,
    lists keys with the relevant prefix and filters to those that
    match the glob pattern.
    """

    uri = os.path.expandvars(uri)

    if _is_s3_uri(uri):
        _ensure_s3_client()

        bucket_name, key = _parse_s3_uri(uri)
        match = _GLOB_CHARACTER_RE.search(key)
        if match:
            key_prefix = key[0:match.start()]
        else:
            key_prefix = key

        key_candidates = list(_S3_CLIENT.iterate_keys(bucket_name, key_prefix))
        matcher = re.compile(_glob_to_re(key)).match
        return [f"s3://{bucket_name}/{k}" for k in key_candidates if matcher(k)]
    else:
        return filesystem_glob(uri)


def _is_s3_uri(uri):
    return uri.startswith("s3://")


def _parse_s3_uri(uri):
    return uri.replace("s3://", "").split("/", 1)


def _ensure_s3_client():
    global _S3_CLIENT
    if _S3_CLIENT is None:
        from stsci_aws_utils.s3 import ConcurrentS3Client
        _S3_CLIENT = ConcurrentS3Client()
        atexit.register(_S3_CLIENT.close)


def _glob_to_re(pat):
    """
    This is the CPython 3.7 source for fnmatch.translate, but tweaked
    to not gobble up path separators with *.  See:
    https://stackoverflow.com/questions/27726545/python-glob-but-against-a-list-of-strings-rather-than-the-filesystem
    """

    i, n = 0, len(pat)
    res = ''
    while i < n:
        c = pat[i]
        i = i+1
        if c == '*':
            # res = res + '.*'
            # Replace original line to exclude path separator:
            res = res + '[^/]*'
        elif c == '?':
            # res = res + '.'
            # Replace original line to exclude path separator:
            res = res + '[^/]'
        elif c == '[':
            j = i
            if j < n and pat[j] == '!':
                j = j+1
            if j < n and pat[j] == ']':
                j = j+1
            while j < n and pat[j] != ']':
                j = j+1
            if j >= n:
                res = res + '\\['
            else:
                stuff = pat[i:j]
                if '--' not in stuff:
                    stuff = stuff.replace('\\', r'\\')
                else:
                    chunks = []
                    k = i+2 if pat[i] == '!' else i+1
                    while True:
                        k = pat.find('-', k, j)
                        if k < 0:
                            break
                        chunks.append(pat[i:k])
                        i = k+1
                        k = k+3
                    chunks.append(pat[i:j])
                    # Escape backslashes and hyphens for set difference (--).
                    # Hyphens that create ranges shouldn't be escaped.
                    stuff = '-'.join(s.replace('\\', r'\\').replace('-', r'\-')
                                     for s in chunks)
                # Escape set operations (&&, ~~ and ||).
                stuff = re.sub(r'([&~|])', r'\\\1', stuff)
                i = j+1
                if stuff[0] == '!':
                    stuff = '^' + stuff[1:]
                elif stuff[0] in ('^', '['):
                    stuff = '\\' + stuff
                res = '%s[%s]' % (res, stuff)
        else:
            res = res + re.escape(c)
    return r'(?s:%s)\Z' % res
