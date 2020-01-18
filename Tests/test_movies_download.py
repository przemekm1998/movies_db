import pytest
from movies_download import MoviesDownload
import json


@pytest.fixture(scope='module')
def movies():
    """ Setup """
    movies = MoviesDownload(file_to_write='plik', titles_to_get='titles.txt')
    print('START')
    print(movies.read_titles())
    yield movies


# Fixtures
def test_read_titles(movies):
    movies.read_titles()

    assert type(movies.titles) is list
    assert movies.titles is not None
    assert 'Kac Wawa' in movies.titles
    assert '1917' in movies.titles
    assert 'Joker' in movies.titles
    assert 'Fajny film' not in movies.titles


def test_get_titles_using_api(movies):
    movies.get_titles_using_api()

    print(movies.results[0])

    with open('get_titles_using_api_json.json', 'r') as correct_file:
        result = correct_file.read()

    print(result)
