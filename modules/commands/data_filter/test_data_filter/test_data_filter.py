import pytest
import sqlite3

from movies_db import DBConfig, DataFilter


@pytest.fixture(scope='module')
def data_filter():
    """ Setup of the data filter before tests """

    db = DBConfig(db_name='resources/movies_test.sqlite')  # Creating new db connection
    data_filter = DataFilter(db=db)

    yield data_filter


def test_handle(data_filter):
    """
    Testing filtering data
    :param data_filter:
    """

    # Filter by title
    results = data_filter.handle(parameter=['title', 'The Godfather'])
    assert results
    assert len(results) == 1  # Only one result expected
    assert results[0]['Title'] == 'The Godfather'  # Data matches expected

    # Filter by language
    results = data_filter.handle(parameter=['language', 'English'])

    assert results
    assert len(results) == 4  # 4 results expected

    assert results[0]['Title'] == 'The Shawshank Redemption'  # Data matches expected
    assert results[1]['Title'] == 'Memento'  # Data matches expected
    assert results[2]['Title'] == 'In Bruges'  # Data matches expected
    assert results[3]['Title'] == 'The Godfather'  # Data matches expected

    # Filter by cast
    results = data_filter.handle(parameter=['cast', 'Tim Robbins'])

    assert results
    assert len(results) == 1

    assert 'Tim Robbins' in results[0]['Cast']
    assert results[0]['Title'] == 'The Shawshank Redemption'

    # No match
    results = data_filter.handle(parameter=['year', '300'])  # No movies made in year 3000

    assert not results

    # Fail case - query by column which doesn't exist
    with pytest.raises(sqlite3.OperationalError) as exec_info:
        results = data_filter.handle(parameter=['metascore', '5'])
    print(exec_info.value)

