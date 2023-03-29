import requests as rq
from dagster import Output, asset, define_asset_job

from data_pipelines.pipelines.usaf_docket.usaf_docket_types import usaf_bases_type


@asset(
    description=(
        "Submits an HTTP get request to the USAF Docket API to retrieve a list of all"
        " bases. The bases API returns a list of bases in XML format."
    ),
    compute_kind="Python",
    code_version="1.0",
    dagster_type=usaf_bases_type,
    group_name="extract",
)
def usaf_docket_bases(context) -> Output:
    """
    Submits an HTTP get request to the USAF Docket API.

    The bases API returns a list of bases in XML format.

    """

    context.log.info("Starting execution of usaf_docket_bases")

    URL: str = "https://legalassistance.law.af.mil/AMJAMS/PublicDocket/baseList.xml"

    try:
        context.log.info(
            f"Submitting HTTP GET request to USAF Docket API at URL: {URL}"
        )

        response: rq.Response = rq.get(URL, timeout=10)

        response.raise_for_status()

        context.log.info(
            "Successfully retrieved response from USAF Docket API. Status code:"
            f" {response.status_code}"
        )

        metadata: dict = {
            "url": URL,
            "status_code": str(response.status_code),
            "response_time": response.elapsed.total_seconds(),
            "response_size": len(response.content),
        }

    except rq.exceptions.RequestException as exception:

        context.log.error(
            "Error occurred while attempting to retrieve response from USAF Docket"
            f" API. Exception: {exception}"
        )

    return Output(value=response, metadata=metadata)


usaf_docket_job = define_asset_job(
    name="usaf_docket_pipeline",
    selection=["usaf_docket_bases"],
    description="Pipeline to retrieve data from the USAF Docket site.",
)
