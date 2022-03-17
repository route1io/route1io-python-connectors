"""AWS

This module contains convenience functionality for working with AWS.
"""
from typing import Union, Sequence, Dict, Tuple

from pathlib import Path

import boto3

FilenameVar = Union[str, Sequence[Union[str, None]]]

def get_most_recent_filename(s3, bucket: str, prefix: str = "") -> str:
    """Return the key name as it appears in s3 bucket of the most recently modified
    file in bucket

    Parameters
    ----------
    s3
        Connection to AWS S3 bucket
    bucket : str
        Name of the bucket that contains data we want
    prefix : str, optional
        Prefix to filter data

    Returns
    -------
    key
        Name of the most recently modified file as it appears in S3 bucket
    """
    # pylint: disable=unsubscriptable-object
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    latest = None
    for page in page_iterator:
        if "Contents" in page:
            latest_test = max(page["Contents"], key=lambda x: x["LastModified"])
            if latest is None or latest_test["LastModified"] > latest["LastModified"]:
                latest = latest_test
    key = latest["Key"]
    return key

def connect_to_s3(aws_access_key_id: str, aws_secret_access_key: str, region_name: str):
    """Returns a connection to s3 bucket via AWS

    Parameters
    ----------
    aws_access_key_id : str
        AWS access key
    aws_secret_access_key : str
        AWS secret access key
    region_name : str
        Default region name

    Returns
    -------
    s3
        Connection to s3 bucket via AWS
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    return s3

def upload_to_s3(s3, filename: Union[str, Sequence[str]], bucket: str,
                 key: Union[str, Sequence[str]] = None) -> None:
    """Uploads a file to AWS s3 bucket

    Parameters
    ----------
    s3
        Connection to s3 bucket
    filename : str
        Local filepath of file to be uploaded
    bucket : str
        Name of s3 bucket to upload file to
    key : str (optional)
        Remote filename to upload file as
    """

    s3.upload_file(
        Filename=filename,
        Bucket=bucket,
        Key=key
    )

def download_from_s3(s3, bucket: str, key: str, filename: str = None) -> str:
    """
    Download file via AWS s3 bucket and return the fpath to the local file

    Parameters
    ----------
    s3
        Connection to s3 bucket
    bucket : str
        Name of s3 bucket to download file from
    key : str
        Remote filename to download from the bucket
    """
    if filename is None:
        filename = key
    s3.download_file(
        Bucket=bucket,
        Key=key,
        Filename=filename
    )
    return filename

def _create_filename_key_map(filename: FilenameVar,
                             key: FilenameVar,
                             filename_required: bool = False,
                             key_required: bool = False) -> Dict[str, str]:
    """Return a dictionary of string pairs mapping key as it appears on AWS to
    local filename"""
    filename = _coerce_input_to_list(filename)
    key = _coerce_input_to_list(key)
    _filenames_and_keys_are_valid_inputs(
        filename=filename,
        key=key,
        filename_required=filename_required,
        key_required=key_required
    )
    if filename_required:
        keys = (filename, key)
    if key_required:

def _filenames_and_keys_are_valid_inputs(filename: Tuple[str],
                                         key: Tuple[str],
                                         filename_required: bool = False,
                                         key_required: bool = False):
    """Validation checks on user input"""
    _validate_lengths(filename, key)
    _validate_input(filename, filename_required, "Filename")
    _validate_input(key, key_required, "Key")

def _validate_lengths(filename: Tuple[str], key: Tuple[str]) -> None:
    """If lengths of both are greater than zero but do not match raise ValueError"""
    filename_num = len(filename)
    key_num = len(key)
    if filename_num > 0 and key_num > 0:
        if filename_num != key_num:
            raise("filename and key cannot both be greater than zero and unequal length as this means the keys won't map together properly")

def _validate_input(seq: Tuple[str], required: bool, name: str) -> None:
    """Validate input is correct otherwise raise ValueError"""
    contains_none = _sequence_contains_none(seq)
    if required and contains_none:
        raise(ValueError(f"{name} cannot contain missing values!"))

def _sequence_contains_none(seq: Tuple[str]) -> bool:
    """Return True if None in sequence else False"""
    return any([val is None for val in seq])

def _coerce_input_to_list(seq: FilenameVar) -> Tuple[str]:
    """Return tuple of values from string or sequence as argument provided by user"""
    if isinstance(seq, str):
        seq = [seq]
    elif seq is None:
        seq = []
    else:
        seq = list(seq)
    return seq