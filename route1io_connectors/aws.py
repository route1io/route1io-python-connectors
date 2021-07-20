"""route1.io AWS connectors

This module contains code for interacting with AWS S3 buckets via Python
"""
from pathlib import Path

import boto3

def connect_to_S3(aws_access_key_id: str, aws_secret_access_key: str, region_name: str):
    """Returns a connection to S3 bucket via AWS

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
        Connection to S3 bucket
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    return s3

def upload_to_S3(
        s3,
        bucket: str,
        local_fpath: str,
        key: str = None,
    ) -> None:
    """Uploads a file to AWS s3 bucket

    Parameters
    ----------
    s3
        Connection to S3 bucket
    bucket : str
        Name of S3 bucket to upload file to
    filename : str
        Local filepath of file to be uploaded
    key : str (optional)
        Remote filename to upload file as
    """
    if key is None:
        key=Path(local_fpath).name
    s3.upload_file(
        Filename=local_fpath,
        Bucket=bucket,
        Key=key
    )

def download_from_S3(s3, bucket: str, key: str, local_fpath: str = None) -> str:
    """
    Download file from an AWS s3 bucket and return the filepath to the local file

    Parameters
    ----------
    s3
        Connection to S3 bucket
    bucket : str
        Name of S3 bucket to download file from
    key : str
        Remote filename to download from the bucket
    local_fpath : str (optional)
        Local filepath to download the file to. Defaults to local directory 
        with key as filename
    """
    if local_fpath is None:
        local_fpath=key
    s3.download_file(
        Bucket=bucket,
        Key=key,
        Filename=local_fpath
    )
    return local_fpath
