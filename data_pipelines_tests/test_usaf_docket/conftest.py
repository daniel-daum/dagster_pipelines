import pytest
import requests 
import responses
from dagster import build_op_context

from data_pipelines.pipelines.usaf_docket.usaf_docket_pipeline import usaf_docket_bases

@pytest.fixture
def mock_bases_api():
    """Mock the usaf_docket_bases api call for testing"""

    context = build_op_context()

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "https://legalassistance.law.af.mil/AMJAMS/PublicDocket/baseList.xml",
            status=200,
            json={"base": [{"base_name": "Eglin AFB", "base_code": "EGL"}]},
        )

        results = usaf_docket_bases(context)

    return results.value




