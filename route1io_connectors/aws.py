"""AWS

This module contains convenience functionality for working with AWS.
"""
from pathlib import Path

import boto3

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

def upload_to_s3(s3, filename: str, bucket: str, key: str = None) -> None:
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
    if key is None:
        key = Path(filename).name
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