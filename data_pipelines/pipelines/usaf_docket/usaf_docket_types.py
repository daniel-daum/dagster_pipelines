from dagster import PythonObjectDagsterType
from requests import Response

usaf_bases_type = PythonObjectDagsterType(python_type=Response)
