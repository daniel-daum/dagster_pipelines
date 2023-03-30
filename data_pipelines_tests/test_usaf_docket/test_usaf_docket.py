from requests import Response
from data_pipelines_tests.test_usaf_docket.conftest import body
import pandas as pd

def test_usaf_dockey_bases(mock_bases_api):
    """Test the usaf_docket_bases api call for testing"""


    assert type(mock_bases_api) == Response
    assert mock_bases_api.status_code == 200
    assert mock_bases_api.text == body



def test_transform_usaf_docket_bases(mock_bases_transformation):

    column_names:list[str] = mock_bases_transformation.columns.tolist()


    assert type(mock_bases_transformation) == pd.DataFrame
    assert len(mock_bases_transformation) == 3
    assert "base" in column_names
    assert "base_name" in column_names
    assert "state_abv" in column_names
    assert "state" in column_names
    assert "phone_number" in column_names

    assert pd.api.types.is_string_dtype(mock_bases_transformation["base"])
    assert pd.api.types.is_string_dtype(mock_bases_transformation["base_name"])
    assert pd.api.types.is_string_dtype(mock_bases_transformation["state_abv"])
    assert pd.api.types.is_string_dtype(mock_bases_transformation["state"])
    assert pd.api.types.is_string_dtype(mock_bases_transformation["phone_number"])





# def test_xml_res(mock_bases_api):

#     print(type(mock_bases_api.text))
#     print(mock_bases_api)
#     print(mock_bases_api.content)
