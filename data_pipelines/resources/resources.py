from dagster import resource
from sqlalchemy import create_engine

from data_pipelines.settings import settings


@resource
def database_dev():
    """
    Creates a connection to the development database.
    """
    engine = create_engine(settings.DBSTR_DEV)

    yield engine

    engine.dispose()
