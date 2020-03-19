import pytest

from movies_db import DBConfig, DataCompare


@pytest.fixture(scope='module')
def data_compare():
    """ Setup of the data filter before tests """

    db = DBConfig(db_name='resources/movies_test.sqlite')  # Creating new db connection
    data_compare = DataCompare(db=db)

    yield data_compare


def test_handle(data_compare):
    """
    Testing comparing data by parameter
    :param data_compare:
    :return:
    """

    # Compare by imdb_rating
    results = data_compare.handle(parameter=['imdb_rating', 'Memento', 'The Godfather'])
    assert results[0]['Title'] == 'The Godfather'

    # Compare by box office
    results = data_compare.handle(parameter=['box_office', 'Memento', 'In Bruges'])
    assert results[0]['Title'] == 'Memento'

    # Compare by runtime
    results = data_compare.handle(parameter=['runtime', 'Gods', 'The Godfather'])
    assert results[0]['Title'] == 'The Godfather'
