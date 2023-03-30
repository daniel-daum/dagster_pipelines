from defusedxml import ElementTree as DET
import xml.etree.ElementTree as ET
import pandas as pd
import requests as rq
from dagster import (
    MarkdownMetadataValue,
    MetadataValue,
    Output,
    asset,
    define_asset_job,
)

from data_pipelines.pipelines.usaf_docket.usaf_docket_types import (
    transfrom_usaf_bases_type,
    usaf_bases_type,
)


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

        metadata: dict[str, str | int | float] = {
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


@asset(
    description="Transforms an XML response into a polars dataframe",
    compute_kind="Python",
    code_version="0.1",
    dagster_type=transfrom_usaf_bases_type,
    group_name="transform",
)
def transform_usaf_docket_bases(context, usaf_docket_bases: rq.Response) -> Output:

    context.log.info("Starting execution of transform_usaf_docket_bases")

    # Use defusedxml to patch XML stdlib to prevent XML attacks
    tree: ET.ElementTree = ET.ElementTree(DET.fromstring(usaf_docket_bases.text))

    root: ET.Element = tree.getroot()

    data: list[dict[str, str]] = []

    context.log.info("Transforming XML response into a pandas dataframe")

    element: ET.Element

    for element in root:
        base: str = str(element.find("base"))
        baseName: str = str(element.find("baseName"))
        stateABV: str = str(element.find("stateAbbrev"))
        statelong: str = str(element.find("stateLongName"))
        phone: str = str(element.find("basePhone"))

        data.append(
            {
                "base": base,
                "base_name": baseName,
                "state_abv": stateABV,
                "state": statelong,
                "phone_number": phone,
            }
        )

    df: pd.DataFrame = pd.DataFrame(data)

    context.log.info("Successfully transformed XML response into a pandas dataframe")

    preview: pd.DataFrame = pd.DataFrame(df.head(30))

    metadata: dict[str, str | int | MarkdownMetadataValue] = {
        "num_rows": len(df),
        "columns": df.columns.tolist(),
        "preview": MetadataValue.md(str(preview.to_markdown())),
    }

    return Output(value=df, metadata=metadata)


usaf_docket_job = define_asset_job(
    name="usaf_docket_pipeline",
    selection=["usaf_docket_bases", "transform_usaf_docket_bases"],
    description="Pipeline to retrieve data from the USAF Docket site.",
)
