import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from core.config import S3_ACCESS_KEY, S3_BUCKET_NAME, S3_ENDPOINT, S3_REGION, S3_SECRET_KEY


s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION,
    config=Config(s3={"addressing_style": "path"}),
)


def ensure_bucket() -> None:
    """Create the Ozone S3 bucket when the backend runs outside Compose."""
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
    except ClientError:
        s3_client.create_bucket(Bucket=S3_BUCKET_NAME)


ensure_bucket()
