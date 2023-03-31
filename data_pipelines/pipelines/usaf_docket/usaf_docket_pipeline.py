import xml.etree.ElementTree as ET
from datetime import datetime as dt

import pandas as pd
import requests as rq
from dagster import (
    AssetMaterialization,
    Failure,
    MarkdownMetadataValue,
    MetadataValue,
    Output,
    asset,
    define_asset_job,
)
from defusedxml import ElementTree as DET

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

        error_statement: str = (
            "Error occurred while attempting to retrieve response from USAF bases URL"
        )

        context.log.error(f"{error_statement}. Exception: {exception}")

        raise Failure(description=error_statement, metadata={"exception": exception})

    return Output(value=response, metadata=metadata)


@asset(
    description="Transforms an XML response into a pandas dataframe",
    compute_kind="Python",
    code_version="1.1",
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
        base: str = str(element.find("base").text)
        base_name: str = str(element.find("baseName").text)
        state_abv: str = str(element.find("stateAbbrev").text)
        state_long: str = str(element.find("stateLongName").text)
        phone: str = str(element.find("basePhone").text)

        data.append(
            {
                "base": base,
                "base_name": base_name,
                "state_abv": state_abv,
                "state": state_long,
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


@asset(
    description="Transforms an XML response into a pandas dataframe",
    compute_kind="Python",
    code_version="0.0",
    group_name="bronze",
    required_resource_keys={"database"},
)
def usaf_bases_raw(context, transform_usaf_docket_bases) -> None:
    """Loads the bases dataframe into the postgres database"""

    context.log.info("Starting execution of usaf_bases_raw")

    transform_usaf_docket_bases["updated_at"] = dt.now()

    try:
        context.log.info(
            "Loading dataframe into the postgres database. Using overwrite pattern."
        )

        transform_usaf_docket_bases.to_sql(
            name="usaf_bases_raw", con=context.resources.database, if_exists="replace"
        )

        context.log.info(
            "Successfully loaded dataframe into the postgres database."
            f" {len(transform_usaf_docket_bases)} rows loaded."
        )

    except Exception as exception:

        error_statement: str = (
            "Error occurred while attempting to load dataframe into the postgres"
            " database."
        )
        context.log.error(f"{error_statement}. Exception: {exception}")

        raise Failure(description=error_statement, metadata={"exception": exception})

    metadata: dict[str, str | int | MarkdownMetadataValue] = {
        "num_rows": len(transform_usaf_docket_bases),
        "columns": transform_usaf_docket_bases.columns.tolist(),
        "preview": MetadataValue.md(str(transform_usaf_docket_bases.to_markdown())),
    }

    context.log_event(
        AssetMaterialization(
            asset_key="usaf_bases_raw",
            description="Persisted result to storage",
            metadata=metadata,
        )
    )


@asset(
    description=(
        "Submits an HTTP get request to the USAF Docket API to retrieve a list of all"
        " charges. The charges API returns a list of bases in XML format."
    ),
    compute_kind="Python",
    code_version="0.0",
    group_name="extract",
)
def usaf_docket_charges(context) -> Output:
    """
    Submits an HTTP get request to the USAF Docket charges API.

    The charges API returns a list of bases in XML format.

    """

    context.log.info("Starting execution of usaf_docket_bases")

    URL: str = "https://legalassistance.law.af.mil/AMJAMS/PublicDocket/chargeList.xml"

    try:
        context.log.info(
            f"Submitting HTTP GET request to USAF Docket charges at URL: {URL}"
        )

        response: rq.Response = rq.get(URL, timeout=10)

        response.raise_for_status()

        context.log.info(
            "Successfully retrieved response from USAF Docket charges API. Status code:"
            f" {response.status_code}"
        )

        metadata: dict[str, str | int | float] = {
            "url": URL,
            "status_code": str(response.status_code),
            "response_time": response.elapsed.total_seconds(),
            "response_size": len(response.content),
        }

    except rq.exceptions.RequestException as exception:

        error_statement: str = (
            "Error occurred while attempting to retrieve response from USAF charges URL"
        )

        context.log.error(f"{error_statement}. Exception: {exception}")

        raise Failure(description=error_statement, metadata={"exception": exception})

    return Output(value=response, metadata=metadata)


@asset(
    description="Transforms an XML response into a pandas dataframe",
    compute_kind="Python",
    code_version="0.0",
    group_name="transform",
)
def transform_usaf_docket_charges(context, usaf_docket_charges: rq.Response) -> Output:
    """Transforms a http response object into a tabular pandas dataframe"""

    tree: ET.ElementTree = ET.ElementTree(DET.fromstring(usaf_docket_charges.text))

    root: ET.Element = tree.getroot()

    data: list[dict[str, str]] = []

    context.log.info("Transforming XML response into a pandas dataframe")

    element: ET.Element

    for element in root:
        code: str = str(element.find("specCode").text)
        article: str = str(element.find("specArticle").text)
        definition: str = str(element.find("specDefinition").text)

        data.append({"code": code, "article": article, "definition": definition})

    df: pd.DataFrame = pd.DataFrame(data)

    context.log.info("Successfully transformed XML response into a pandas dataframe")

    preview: pd.DataFrame = pd.DataFrame(df.head(30))

    metadata: dict[str, str | int | MarkdownMetadataValue] = {
        "num_rows": len(df),
        "columns": df.columns.tolist(),
        "preview": MetadataValue.md(str(preview.to_markdown())),
    }

    return Output(value=df, metadata=metadata)


@asset(
    description="Transforms an XML response into a pandas dataframe",
    compute_kind="Python",
    code_version="0.0",
    group_name="bronze",
    required_resource_keys={"database"},
)
def usaf_charges_raw(context, transform_usaf_docket_charges) -> None:
    """Loads the bases dataframe into the postgres database"""

    context.log.info("Starting execution of usaf_charges_raw")

    transform_usaf_docket_charges["updated_at"] = dt.now()

    try:
        context.log.info(
            "Loading dataframe into the postgres database. Using overwrite pattern."
        )

        transform_usaf_docket_charges.to_sql(
            name="usaf_charges_raw", con=context.resources.database, if_exists="replace"
        )

        context.log.info(
            "Successfully loaded dataframe into the postgres database."
            f" {len(transform_usaf_docket_charges)} rows loaded."
        )

    except Exception as exception:

        error_statement: str = (
            "Error occurred while attempting to load dataframe into the postgres"
            " database."
        )
        context.log.error(f"{error_statement}. Exception: {exception}")

        raise Failure(description=error_statement, metadata={"exception": exception})

    metadata: dict[str, str | int | MarkdownMetadataValue] = {
        "num_rows": len(transform_usaf_docket_charges),
        "columns": transform_usaf_docket_charges.columns.tolist(),
        "preview": MetadataValue.md(str(transform_usaf_docket_charges.to_markdown())),
    }

    context.log_event(
        AssetMaterialization(
            asset_key="usaf_charges_raw",
            description="Persisted result to storage",
            metadata=metadata,
        )
    )


@asset(
    description=(
        "Submits an HTTP get request to the USAF Docket API to retrieve a list of all"
        " cases. The charges API returns a list of cases in XML format."
    ),
    compute_kind="Python",
    code_version="0.0",
    group_name="extract",
)
def usaf_docket_cases(context) -> Output:
    """
    Submits an HTTP get request to the USAF Docket charges API.

    The cases API returns a list of cases in XML format.

    """

    context.log.info("Starting execution of usaf_docket_cases")

    URL: str = "https://legalassistance.law.af.mil/AMJAMS/PublicDocket/caseList.1.7.xml"

    try:
        context.log.info(
            f"Submitting HTTP GET request to USAF Docket cases at URL: {URL}"
        )

        response: rq.Response = rq.get(URL, timeout=10)

        response.raise_for_status()

        context.log.info(
            "Successfully retrieved response from USAF Docket cases API. Status code:"
            f" {response.status_code}"
        )

        metadata: dict[str, str | int | float] = {
            "url": URL,
            "status_code": str(response.status_code),
            "response_time": response.elapsed.total_seconds(),
            "response_size": len(response.content),
        }

    except rq.exceptions.RequestException as exception:

        error_statement: str = (
            "Error occurred while attempting to retrieve response from USAF cases URL"
        )

        context.log.error(f"{error_statement}. Exception: {exception}")

        raise Failure(description=error_statement, metadata={"exception": exception})

    return Output(value=response, metadata=metadata)


@asset(
    description="Transforms an XML response into a pandas dataframe",
    compute_kind="Python",
    code_version="0.0",
    group_name="transform",
)
def transform_usaf_docket_cases(context, usaf_docket_cases: rq.Response) -> Output:
    """Transforms a http response object into a tabular pandas dataframe"""

    tree: ET.ElementTree = ET.fromstring(usaf_docket_cases.text)

    data: list[dict[str, str]] = []

    context.log.info("Transforming XML response into a pandas dataframe")

    element: ET.Element
    branch: ET.Element

    for element in tree:
        for branch in element:

            data.append({branch.tag: branch.attrib})

    df: pd.DataFrame = pd.json_normalize(data)

    context.log.info("Successfully transformed XML response into a pandas dataframe")

    preview: pd.DataFrame = pd.DataFrame(df.head(30))

    metadata: dict[str, str | int | MarkdownMetadataValue] = {
        "num_rows": len(df),
        "columns": df.columns.tolist(),
        "preview": MetadataValue.md(str(preview.to_markdown())),
    }

    return Output(value=df, metadata=metadata)


@asset(
    description="Transforms an XML response into a pandas dataframe",
    compute_kind="Python",
    code_version="0.0",
    group_name="bronze",
    required_resource_keys={"database"},
)
def usaf_cases_raw(context, transform_usaf_docket_cases) -> None:
    """Loads the bases dataframe into the postgres database"""

    context.log.info("Starting execution of usaf_bases_raw")

    transform_usaf_docket_cases["updated_at"] = dt.now()

    try:
        context.log.info(
            "Loading dataframe into the postgres database. Using overwrite pattern."
        )

        transform_usaf_docket_cases.to_sql(
            name="usaf_cases_raw", con=context.resources.database, if_exists="replace"
        )

        context.log.info(
            "Successfully loaded dataframe into the postgres database."
            f" {len(transform_usaf_docket_cases)} rows loaded."
        )

    except Exception as exception:

        error_statement: str = (
            "Error occurred while attempting to load dataframe into the postgres"
            " database."
        )
        context.log.error(f"{error_statement}. Exception: {exception}")

        raise Failure(description=error_statement, metadata={"exception": exception})

    metadata: dict[str, str | int | MarkdownMetadataValue] = {
        "num_rows": len(transform_usaf_docket_cases),
        "columns": transform_usaf_docket_cases.columns.tolist(),
        "preview": MetadataValue.md(str(transform_usaf_docket_cases.to_markdown())),
    }

    context.log_event(
        AssetMaterialization(
            asset_key="usaf_bases_raw",
            description="Persisted result to storage",
            metadata=metadata,
        )
    )


usaf_docket_job = define_asset_job(
    name="usaf_docket_pipeline",
    selection=[
        "usaf_docket_bases",
        "transform_usaf_docket_bases",
        "usaf_bases_raw",
        "usaf_docket_charges",
        "transform_usaf_docket_charges",
        "usaf_charges_raw",
        "usaf_docket_cases",
        "transform_usaf_docket_cases",
        "usaf_cases_raw",
    ],
    description="Pipeline to retrieve data from the USAF Docket site.",
)
