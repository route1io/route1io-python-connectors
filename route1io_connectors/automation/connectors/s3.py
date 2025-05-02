import os

from route1io_connectors import aws

def extract(config):
    # Connect to S3
    s3 = aws.connect_to_s3(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION")
    )

    filename = os.path.join(
        os.environ.get("ROUTE1_WORKING_DIR"),
        config['filename']
    )

    # Ensure local directories exist for filename
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Download
    fpath = aws.download_from_s3(
        s3=s3,
        bucket=config['bucket'],
        key=config['key'],
        filename=filename
    )
    return fpath

def load(config):
    # Connect to S3
    s3 = aws.connect_to_s3(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION")
    )

    filename = os.path.join(
        os.environ.get("ROUTE1_WORKING_DIR"),
        config['filename']
    )

    # Ensure local directories exist for filename
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Download
    fpath = aws.upload_to_s3(
        s3=s3,
        bucket=config['bucket'],
        key=config['key'],
        filename=filename
    )
    return fpath