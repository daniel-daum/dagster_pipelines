from dagster import Definitions, load_assets_from_package_module

from data_pipelines.pipelines import usaf_docket, usaf_docket_job

usaf_docket_assets = load_assets_from_package_module(usaf_docket)


defs = Definitions(assets=[*usaf_docket_assets], jobs=[usaf_docket_job])
