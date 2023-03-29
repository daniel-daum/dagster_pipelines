from requests import Response

def test_usaf_dockey_bases(mock_bases_api):
    """Test the usaf_docket_bases api call for testing"""


    assert type(mock_bases_api) == Response
    assert mock_bases_api.status_code == 200
    assert mock_bases_api.json() == {"base": [{"base_name": "Eglin AFB", "base_code": "EGL"}]}