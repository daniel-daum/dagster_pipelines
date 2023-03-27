from dagster import Definitions


from data_pipelines.pipelines.usaf_docket.usaf_docket import hello

defs = Definitions(assets=[hello])