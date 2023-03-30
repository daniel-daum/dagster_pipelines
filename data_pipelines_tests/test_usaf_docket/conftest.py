import pytest
import requests 
import responses
from dagster import build_op_context

from data_pipelines.pipelines.usaf_docket.usaf_docket_pipeline import usaf_docket_bases, transform_usaf_docket_bases

body:str = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<bases xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
	<baseInfo>
		<base>TRC2</base>
		<baseName>Peterson-Schreiver Garrison</baseName>
		<stateAbbrev>CO</stateAbbrev>
		<stateLongName>Colorado</stateLongName>
		<basePhone>719-556-5815</basePhone>
	</baseInfo>
	<baseInfo>
		<base>TRC4</base>
		<baseName>Vandenberg SFB</baseName>
		<stateAbbrev>CA</stateAbbrev>
		<stateLongName>California</stateLongName>
		<basePhone>805-606-5395</basePhone>
	</baseInfo>
	<baseInfo>
		<base>TRC5</base>
		<baseName>Maxwell AFB</baseName>
		<stateAbbrev>AL</stateAbbrev>
		<stateLongName>Alabama</stateLongName>
		<basePhone>334-953-3779</basePhone>
	</baseInfo>
</bases>

"""



@pytest.fixture
def mock_bases_api():
    """Mock the usaf_docket_bases api call for testing"""

    context = build_op_context()

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "https://legalassistance.law.af.mil/AMJAMS/PublicDocket/baseList.xml",
            status=200,
            body=body,

        )

        results = usaf_docket_bases(context)


    return results.value



@pytest.fixture
def mock_bases_transformation(mock_bases_api):

    context = build_op_context()

    results = transform_usaf_docket_bases(context, mock_bases_api)

    return results.value