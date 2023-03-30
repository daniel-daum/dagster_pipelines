from dagster import PythonObjectDagsterType
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from requests import Response

usaf_bases_type = PythonObjectDagsterType(python_type=Response)


transfrom_usaf_bases_type = create_dagster_pandas_dataframe_type(
    name="transfrom_usaf_bases_type",
    columns=[
        PandasColumn.string_column("base"),
        PandasColumn.string_column("base_name"),
        PandasColumn.string_column("state_abv"),
        PandasColumn.string_column("state"),
        PandasColumn.string_column("phone_number"),
    ],
)
