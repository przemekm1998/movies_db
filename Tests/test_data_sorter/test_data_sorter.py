import pytest
import sqlite3

from movies_db import DBConfig, DataSorter


@pytest.fixture(scope='module')
def data_sorter():
    """ Setup of the data sorter before tests """

    db = DBConfig(db_name='resources/movies_test.sqlite')  # Creating new db connection
    data_sorter = DataSorter(db=db)

    yield data_sorter


def test_handle(data_sorter):
    """
    Testing sorting data by parameter
    :param data_sorter:
    """

    # Query by year
    results = data_sorter.handle('year')
    assert results
    assert len(results) == 5

    assert results[0]['Title'] == 'Gods'
    assert results[1]['Title'] == 'In Bruges'
    assert results[2]['Title'] == 'Memento'
    assert results[3]['Title'] == 'The Shawshank Redemption'
    assert results[4]['Title'] == 'The Godfather'

    # Query by imdb rating
    results = data_sorter.handle('imdb_rating')
    assert results
    assert len(results) == 5

    assert results[0]['Title'] == 'The Shawshank Redemption'
    assert results[1]['Title'] == 'The Godfather'
    assert results[2]['Title'] == 'Memento'
    assert results[3]['Title'] == 'In Bruges'
    assert results[4]['Title'] == 'Gods'

    # Query by box office
    results = data_sorter.handle('box_office')
    assert results
    assert len(results) == 5

    assert results[0]['Title'] == 'Memento'
    assert results[1]['Title'] == 'In Bruges'
    assert results[2]['box_office'] is None
    assert results[3]['box_office'] is None
    assert results[4]['box_office'] is None

    # Fail case - sort by column that does'nt exist
    with pytest.raises(sqlite3.OperationalError) as exec_info:
        no_result = data_sorter.handle('niema')  # No column /niema/
    print(exec_info.value)
