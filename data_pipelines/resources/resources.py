import boto3
from dagster import resource
from dagster_aws.s3.io_manager import s3_pickle_io_manager
from sqlalchemy import create_engine

from data_pipelines.settings import settings

session = boto3.session.Session(
    region_name="us-east-1",
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
)

s3 = session.client("s3")


@resource
def warehouse():
    """
    Creates a connection to the development warehouse.
    """
    engine = create_engine(settings.WAREHOUSE)

    yield engine

    engine.dispose()


s3_io_manager_dev = s3_pickle_io_manager.configured(
    {"s3_bucket": "dagster-io-assets", "s3_prefix": "assets/dev"}
)

s3_io_manager_stage = s3_pickle_io_manager.configured(
    {"s3_bucket": "dagster-io-assets", "s3_prefix": "assets/stage"}
)

s3_io_manager_prod = s3_pickle_io_manager.configured(
    {"s3_bucket": "dagster-io-assets", "s3_prefix": "assets/prod"}
)

RESOURCES_DEV = {"s3": s3, "io_manager": s3_io_manager_dev, "warehouse": warehouse}

RESOURCES_STAGE = {"s3": s3, "io_manager": s3_io_manager_stage, "warehouse": warehouse}

RESOURCES_PROD = {"s3": s3, "io_manager": s3_io_manager_prod, "warehouse": warehouse}
