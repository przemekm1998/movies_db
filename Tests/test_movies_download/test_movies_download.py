import pytest
from movies_db import DataDownload
import json


def test_titles_txt():
    # Testing the case when file to read from doens't exist
    with pytest.raises(FileNotFoundError) as exec_info:
        movies = DataDownload(titles_to_get='titles1.txt')
        movies.read_titles()
    print(exec_info.value)

    # # Testing the case when file to read from is empty
    # with pytest.raises(IndexError) as exec_info:
    # movies = DataDownload(titles_to_get='titlest_empty.txt')
    # movies.read_titles()
    # results = movies.get_titles_using_api()
    # print(exec_info.value)


@pytest.fixture(scope='module')
def movies():
    """ Setup """
    movies = DataDownload(titles_to_get='titlest_empty.txt')
    yield movies


# Fixtures
def test_read_titles(movies):
    """
    Testing reading titles from text file
    :param movies:
    """
    movies.read_titles()

    assert type(movies.titles) is list
    assert movies.titles is not None
    assert 'Kac Wawa' in movies.titles
    assert '1917' in movies.titles
    assert 'Joker' in movies.titles
    assert 'Fajny film' not in movies.titles


def test_get_titles_using_api(movies):
    """
    Testing the part of the result json file
    :param movies:
    """
    results = movies.get_titles_using_api()

    with open('get_titles_using_api_json.json', 'r') as correct_file:
        correct_json = json.load(correct_file)

    assert correct_json == results[0]
