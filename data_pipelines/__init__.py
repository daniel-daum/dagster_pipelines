from dagster import Definitions, file_relative_path, load_assets_from_package_module
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

from data_pipelines.pipelines import (
    usaf_docket,
    usaf_docket_job,
    usaf_docket_weekly_schedule,
)
from data_pipelines.resources import RESOURCES_DEV, RESOURCES_STAGE, RESOURCES_PROD
from data_pipelines.settings import settings

DBT_PROJECT_PATH = file_relative_path(__file__, "../dbt_pipelines")
DBT_PROFILES = file_relative_path(__file__, "../dbt_pipelines/config")

usaf_docket_assets = load_assets_from_package_module(usaf_docket)

dbt = dbt_cli_resource.configured(
    {"project_dir": DBT_PROJECT_PATH, "profiles_dir": DBT_PROFILES}
)

RESOURCES_DEV["dbt"] = dbt
RESOURCES_STAGE["dbt"] = dbt
RESOURCES_PROD["dbt"] = dbt

resources_by_deployment_name = {
    "dev": RESOURCES_DEV,
    "stage": RESOURCES_STAGE,
    "prod": RESOURCES_PROD,
}

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES,
    use_build_command=True,
    key_prefix="usaf_docket",
)

defs = Definitions(
    assets=[*usaf_docket_assets, *dbt_assets],
    jobs=[usaf_docket_job],
    resources=resources_by_deployment_name[settings.TARGET],
    schedules=[usaf_docket_weekly_schedule],
)
