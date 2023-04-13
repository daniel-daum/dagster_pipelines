import uuid
import xml.etree.ElementTree as ET
from datetime import datetime as dt

import pandas as pd
import requests as rq
from dagster import (
    AssetIn,
    AssetMaterialization,
    AssetObservation,
    Failure,
    MarkdownMetadataValue,
    MetadataValue,
    Output,
    asset,
    define_asset_job,
    ScheduleDefinition,
)
from defusedxml import ElementTree as DET

from data_pipelines.pipelines.usaf_docket.usaf_docket_types import (
    transfrom_usaf_bases_type,
    usaf_bases_type,
)
from data_pipelines.pipelines.usaf_docket.usaf_docket_utilities import surrogate_key


@asset(
    description=(
        "Submits an HTTP get request to the USAF Docket API to retrieve a list of all"
        " bases. The bases API returns a list of bases in XML format."
    ),
    compute_kind="Python",
    code_version="1.0",
    dagster_type=usaf_bases_type,
    group_name="extract",
    key_prefix=["usaf_docket"],
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
    key_prefix=["usaf_docket"],
    ins={"usaf_docket_bases": AssetIn(key=["usaf_docket", "usaf_docket_bases"])},
)
def transform_usaf_docket_bases(context, usaf_docket_bases: rq.Response) -> Output:
    """"""

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

    df_cases_surrogate_columns: list[str] = ["base", "base_name"]

    # generate a primary key, which is a surrogate id based on the columns listed above
    df = surrogate_key(df, df_cases_surrogate_columns)

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
    required_resource_keys={"warehouse"},
    group_name="bronze",
    key_prefix=["usaf_docket"],
    ins={
        "transform_usaf_docket_bases": AssetIn(
            key=["usaf_docket", "transform_usaf_docket_bases"]
        )
    },
)
def usaf_bases_raw(context, transform_usaf_docket_bases) -> None:
    """Loads the bases dataframe into the postgres warehouse"""

    context.log.info("Starting execution of usaf_bases_raw")

    try:
        context.log.info(
            "Loading dataframe into the postgres warehouse. Using overwrite pattern."
        )

        transform_usaf_docket_bases["updated_at"] = dt.now()

        transform_usaf_docket_bases.to_sql(
            name="usaf_bases_raw",
            con=context.resources.warehouse,
            if_exists="replace",
            schema="dwh_bronze",
            index=False,
        )

        context.log.info(
            "Successfully loaded dataframe into the postgres warehouse."
            f" {len(transform_usaf_docket_bases)} rows loaded."
        )

    except Exception as exception:
        error_statement: str = (
            "Error occurred while attempting to load dataframe into the postgres"
            " warehouse."
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
            asset_key="usaf_docket/usaf_bases_raw",
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
    key_prefix=["usaf_docket"],
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
    key_prefix=["usaf_docket"],
    ins={"usaf_docket_charges": AssetIn(key=["usaf_docket", "usaf_docket_charges"])},
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

    df_charges_surrogate_columns: list[str] = ["code", "article", "definition"]

    # generate a primary key, which is a surrogate id based on the columns listed above
    df = surrogate_key(df, df_charges_surrogate_columns)

    df = df.drop_duplicates(subset="code")

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
    required_resource_keys={"warehouse"},
    key_prefix=["usaf_docket"],
    ins={
        "transform_usaf_docket_charges": AssetIn(
            key=["usaf_docket", "transform_usaf_docket_charges"]
        )
    },
)
def usaf_charges_raw(context, transform_usaf_docket_charges) -> None:
    """Loads the bases dataframe into the postgres warehouse"""

    context.log.info("Starting execution of usaf_charges_raw")

    transform_usaf_docket_charges["updated_at"] = dt.now()

    try:
        context.log.info(
            "Loading dataframe into the postgres warehouse. Using overwrite pattern."
        )

        transform_usaf_docket_charges.to_sql(
            name="usaf_charges_raw",
            con=context.resources.warehouse,
            if_exists="replace",
            schema="dwh_bronze",
            index=False,
        )

        context.log.info(
            "Successfully loaded dataframe into the postgres warehouse."
            f" {len(transform_usaf_docket_charges)} rows loaded."
        )

    except Exception as exception:
        error_statement: str = (
            "Error occurred while attempting to load dataframe into the postgres"
            " warehouse."
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
            asset_key="usaf_docket/usaf_charges_raw",
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
    key_prefix=["usaf_docket"],
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
    code_version="0.2",
    group_name="transform",
    key_prefix=["usaf_docket"],
    ins={"usaf_docket_cases": AssetIn(key=["usaf_docket", "usaf_docket_cases"])},
)
def transform_usaf_docket_cases(
    context, usaf_docket_cases: rq.Response
) -> dict[str, pd.DataFrame]:
    """Transforms a http response object into a tabular pandas dataframe"""

    root: ET.ElementTree = ET.fromstring(usaf_docket_cases.text)

    element_data: list[dict[str, str]] = []
    sub_element_data: list[dict[str, str]] = []

    context.log.info("Transforming XML response into a pandas dataframe")

    element: ET.Element

    # loop through each element, and sub element and parse data
    for tree in root:
        for element in tree:
            # Generate a uniue id for each element
            id: str = str(uuid.uuid1())

            element.attrib["fk_id"] = id

            case_info: dict = {element.tag: element.attrib}

            for sub_element in element:
                sub_element.attrib["case_id"] = id

                sub_element_data.append(sub_element.attrib)

            element_data.append(case_info)

    case_charges = []
    case_personnel = []

    # Loop through the sub elements and parse data

    for values in sub_element_data:
        if "code" in values:
            case_charge_data = {
                "case_id": values.get("case_id"),
                "code": values.get("code"),
                "prefix": values.get("prefix"),
            }

            case_charges.append(case_charge_data)

        else:
            case_personnel_data = {
                "case_id": values.get("case_id"),
                "order": values.get("order"),
                "title": values.get("title"),
                "name": values.get("name"),
            }

            case_personnel.append(case_personnel_data)

    df_case_info = pd.json_normalize(element_data)
    df_case_charges = pd.DataFrame(case_charges)
    df_case_personnel = pd.DataFrame(case_personnel)

    context.log.info("Successfully transformed XML response into pandas dataframes")

    df_case_info = df_case_info[
        df_case_info["caseInfo.baseCode"].notna()
    ]  # drop a couple completly irrelevant rows

    df_case_info_surrogate_columns: list[str] = ["caseInfo.fk_id"]
    df_case_charges_surrogate_columns: list[str] = ["case_id", "code"]
    df_case_personnel_surrogate_columns: list[str] = [
        "case_id",
        "order",
        "title",
        "name",
    ]

    # generate a primary key, which is a surrogate id based on the columns listed above
    df_case_info = surrogate_key(df_case_info, df_case_info_surrogate_columns)
    df_case_charges = surrogate_key(df_case_charges, df_case_charges_surrogate_columns)
    df_case_personnel = surrogate_key(
        df_case_personnel, df_case_personnel_surrogate_columns
    )

    context.log.info("Added surrogate keys to each dataframe")

    dataframes: dict[str, pd.DataFrame] = {
        "usaf_cases_info_raw": df_case_info,
        "usaf_cases_charges_raw": df_case_charges,
        "usaf_cases_personnel_raw": df_case_personnel,
    }

    preview_info: pd.DataFrame = pd.DataFrame(df_case_info.head(30))
    preview_charges: pd.DataFrame = pd.DataFrame(df_case_charges.head(30))
    preview_personnel: pd.DataFrame = pd.DataFrame(df_case_personnel.head(30))

    case_info_metadata: dict[str, str | int | MarkdownMetadataValue] = {
        "case_info_rows": len(df_case_info),
        "case_info_columns": df_case_info.columns.tolist(),
        "case_info_preview": MetadataValue.md(str(preview_info.to_markdown())),
    }

    case_charges_metadata: dict[str, str | int | MarkdownMetadataValue] = {
        "case_charges_rows": len(df_case_charges),
        "case_charges_columns": df_case_charges.columns.tolist(),
        "case_charges_preview": MetadataValue.md(str(preview_charges.to_markdown())),
    }

    case_personnel_metadata: dict[str, str | int | MarkdownMetadataValue] = {
        "case_personnel_rows": len(df_case_personnel),
        "case_personnel_columns": df_case_personnel.columns.tolist(),
        "case_personnel_preview": MetadataValue.md(
            str(preview_personnel.to_markdown())
        ),
    }

    context.log_event(
        AssetObservation(
            asset_key="usaf_docket/usaf_cases_info_raw", metadata=case_info_metadata
        )
    )
    context.log_event(
        AssetObservation(
            asset_key="usaf_docket/usaf_cases_charges_raw",
            metadata=case_charges_metadata,
        )
    )
    context.log_event(
        AssetObservation(
            asset_key="usaf_docket/usaf_cases_personnel_raw",
            metadata=case_personnel_metadata,
        )
    )

    return dataframes


@asset(
    description="Transforms an XML response into a pandas dataframe",
    compute_kind="Python",
    code_version="0.0",
    group_name="bronze",
    required_resource_keys={"warehouse"},
    key_prefix=["usaf_docket"],
    ins={
        "transform_usaf_docket_cases": AssetIn(
            key=["usaf_docket", "transform_usaf_docket_cases"]
        )
    },
)
def usaf_cases_info_raw(
    context, transform_usaf_docket_cases: dict[str, pd.DataFrame]
) -> None:
    """Loads the bases dataframe into the postgres warehouse"""

    context.log.info("Starting execution of usaf_cases_info_raw")

    df = transform_usaf_docket_cases.get("usaf_cases_info_raw")

    df["updated_at"] = dt.now()

    try:
        context.log.info(
            "Loading dataframe into the postgres warehouse. Using overwrite pattern."
        )

        df.to_sql(
            name="usaf_cases_info_raw",
            con=context.resources.warehouse,
            if_exists="replace",
            schema="dwh_bronze",
            index=False,
        )

        context.log.info(
            "Successfully loaded dataframe into the postgres warehouse."
            f" {len(df)} rows loaded."
        )

    except Exception as exception:
        error_statement: str = (
            "Error occurred while attempting to load dataframe into the postgres"
            " warehouse."
        )
        context.log.error(f"{error_statement}. Exception: {exception}")

        raise Failure(description=error_statement, metadata={"exception": exception})

    metadata: dict[str, str | int | MarkdownMetadataValue] = {
        "num_rows": len(df),
        "columns": df.columns.tolist(),
        "preview": MetadataValue.md(str(df.to_markdown())),
    }

    context.log_event(
        AssetMaterialization(
            asset_key="usaf_docket/usaf_cases_info_raw",
            description="Persisted result to storage",
            metadata=metadata,
        )
    )


@asset(
    description="Transforms an XML response into a pandas dataframe",
    compute_kind="Python",
    code_version="0.0",
    group_name="bronze",
    required_resource_keys={"warehouse"},
    key_prefix=["usaf_docket"],
    ins={
        "transform_usaf_docket_cases": AssetIn(
            key=["usaf_docket", "transform_usaf_docket_cases"]
        )
    },
)
def usaf_cases_charges_raw(context, transform_usaf_docket_cases) -> None:
    """Loads the bases dataframe into the postgres warehouse"""

    context.log.info("Starting execution of usaf_cases_charges_raw")

    df = transform_usaf_docket_cases.get("usaf_cases_charges_raw")

    df["updated_at"] = dt.now()

    try:
        context.log.info(
            "Loading dataframe into the postgres warehouse. Using overwrite pattern."
        )

        df.to_sql(
            name="usaf_cases_charges_raw",
            con=context.resources.warehouse,
            if_exists="replace",
            schema="dwh_bronze",
            index=False,
        )

        context.log.info(
            "Successfully loaded dataframe into the postgres warehouse."
            f" {len(df)} rows loaded."
        )

    except Exception as exception:
        error_statement: str = (
            "Error occurred while attempting to load dataframe into the postgres"
            " warehouse."
        )
        context.log.error(f"{error_statement}. Exception: {exception}")

        raise Failure(description=error_statement, metadata={"exception": exception})

    metadata: dict[str, str | int | MarkdownMetadataValue] = {
        "num_rows": len(df),
        "columns": df.columns.tolist(),
        "preview": MetadataValue.md(str(df.to_markdown())),
    }

    context.log_event(
        AssetMaterialization(
            asset_key="usaf_docket/usaf_cases_charges_raw",
            description="Persisted result to storage",
            metadata=metadata,
        )
    )


@asset(
    description="Transforms an XML response into a pandas dataframe",
    compute_kind="Python",
    code_version="0.0",
    group_name="bronze",
    required_resource_keys={"warehouse"},
    key_prefix=["usaf_docket"],
    ins={
        "transform_usaf_docket_cases": AssetIn(
            key=["usaf_docket", "transform_usaf_docket_cases"]
        )
    },
)
def usaf_cases_personnel_raw(context, transform_usaf_docket_cases) -> None:
    """Loads the bases dataframe into the postgres warehouse"""

    context.log.info("Starting execution of usaf_cases_personnel_raw")

    df = transform_usaf_docket_cases.get("usaf_cases_personnel_raw")

    df["updated_at"] = dt.now()

    try:
        context.log.info(
            "Loading dataframe into the postgres warehouse. Using overwrite pattern."
        )

        df.to_sql(
            name="usaf_cases_personnel_raw",
            con=context.resources.warehouse,
            if_exists="replace",
            schema="dwh_bronze",
            index=False,
        )

        context.log.info(
            "Successfully loaded dataframe into the postgres warehouse."
            f" {len(df)} rows loaded."
        )

    except Exception as exception:
        error_statement: str = (
            "Error occurred while attempting to load dataframe into the postgres"
            " warehouse."
        )
        context.log.error(f"{error_statement}. Exception: {exception}")

        raise Failure(description=error_statement, metadata={"exception": exception})

    metadata: dict[str, str | int | MarkdownMetadataValue] = {
        "num_rows": len(df),
        "columns": df.columns.tolist(),
        "preview": MetadataValue.md(str(df.to_markdown())),
    }

    context.log_event(
        AssetMaterialization(
            asset_key="usaf_docket/usaf_cases_personnel_raw",
            description="Persisted result to storage",
            metadata=metadata,
        )
    )


usaf_docket_job = define_asset_job(
    name="usaf_docket_pipeline",
    selection=[
        "usaf_docket/usaf_docket_bases",
        "usaf_docket/transform_usaf_docket_bases",
        "usaf_docket/usaf_bases_raw",
        "usaf_docket/usaf_docket_charges",
        "usaf_docket/transform_usaf_docket_charges",
        "usaf_docket/usaf_charges_raw",
        "usaf_docket/usaf_docket_cases",
        "usaf_docket/transform_usaf_docket_cases",
        "usaf_docket/usaf_cases_info_raw",
        "usaf_docket/usaf_cases_charges_raw",
        "usaf_docket/usaf_cases_personnel_raw",
        "usaf_docket/usaf_bases_raw_snapshot",
        "usaf_docket/usaf_cases_charges_raw_snapshot",
        "usaf_docket/usaf_cases_personnel_raw_snapshot",
        "usaf_docket/usaf_cases_info_raw_snapshot",
        "usaf_docket/usaf_charges_raw_snapshot",
        "usaf_docket/silver/stg_usaf_bases",
        "usaf_docket/silver/stg_usaf_charges",
        "usaf_docket/silver/stg_usaf_cases_active",
        "usaf_docket/silver/stg_usaf_cases_inactive",
        "usaf_docket/silver/stg_usaf_cases_personnel",
        "usaf_docket/silver/stg_usaf_cases_charges",
        "usaf_docket/silver/usaf_charges",
        "usaf_docket/silver/usaf_cases_active",
        "usaf_docket/silver/usaf_cases_inactive",
        "usaf_docket/gold/active_cases",
        "usaf_docket/gold/inactive_cases",
        # "usaf_docket/upload_case_data"
    ],
    description="Pipeline to retrieve data from the USAF Docket site.",
)

usaf_docket_weekly_schedule = ScheduleDefinition(
    job=usaf_docket_job, cron_schedule="0 6 * * *", execution_timezone="US/Eastern"
)
