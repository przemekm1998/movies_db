import pytest
from movies_db import DataDownloader
import json


# def test_titles_txt():
#     # Testing the case when file to read from doens't exist
#     with pytest.raises(FileNotFoundError) as exec_info:
#         movies = DataDownload(titles_to_get='titles1.txt')
#         movies.read_titles()
#     print(exec_info.value)
#
#     # # Testing the case when file to read from is empty
#     # with pytest.raises(IndexError) as exec_info:
#     # movies = DataDownload(titles_to_get='titlest_empty.txt')
#     # movies.read_titles()
#     # results = movies.get_titles_using_api()
#     # print(exec_info.value)

@pytest.fixture(scope='module')
def data_downloader():
    """ Downloader for tests """

    downloader = DataDownloader()
    yield downloader


@pytest.fixture(scope='module')
def titles():
    """ Titles to download data """

    titles = ['The Shawshank Redemption', 'Memento', 'In Bruges', 'Gods', 'The Godfather']
    yield titles


@pytest.fixture(scope='module')
def fail_titles():
    """ Titles to download data """

    titles = ['Niema1', 'Niema2']
    yield titles


def test_download_data(data_downloader, titles, fail_titles):
    """
    Testing downloading json data
    """

    # Checking if data for all movies is downloaded
    # results = data_downloader.download_data(titles)
    #
    # assert results is not None
    # assert len(results) == 5
    #
    # assert results[0]['Title'] == 'The Shawshank Redemption'
    # assert results[1]['Title'] == 'Memento'
    # assert results[2]['Title'] == 'In Bruges'
    # assert results[3]['Title'] == 'Gods'
    # assert results[4]['Title'] == 'The Godfather'
    #
    # # Checking json data corectness
    # with open('resources/json_movies.json', 'r') as correct_file:
    #     correct_json = json.load(correct_file)
    #
    # assert correct_json == results[0]

    # Fails
    results = data_downloader.download_data(fail_titles)
