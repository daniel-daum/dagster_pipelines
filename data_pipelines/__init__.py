from dagster import Definitions, load_assets_from_package_module, file_relative_path
from dagster_dbt import load_assets_from_dbt_project, dbt_cli_resource

from data_pipelines.pipelines import usaf_docket
from data_pipelines.resources import database_dev

DBT_PROJECT_PATH = file_relative_path(__file__,"../dbt_pipelines")
DBT_PROFILES = file_relative_path(__file__,"../dbt_pipelines/config")

dbt = dbt_cli_resource.configured({"project_dir":DBT_PROJECT_PATH, "profiles_dir":DBT_PROFILES})

dbt_assets = load_assets_from_dbt_project(project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES)
usaf_docket_assets = load_assets_from_package_module(usaf_docket)



defs = Definitions(
    assets=[*usaf_docket_assets, *dbt_assets],
    # jobs=[usaf_docket_job],
    resources={"database": database_dev, "dbt":dbt},
)
