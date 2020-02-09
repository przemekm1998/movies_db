import sqlite3

import pytest
from movies_db import DBConfig, DataDownload


@pytest.fixture(scope='module')
def database():
    """ Setup of the database before tests """

    # Setup - creating new database
    db = DBConfig(db_name='movies.db')
    db.db_create()

    yield db

    # Teardown - deleting the tested table
    drop_table_statement = "DROP TABLE movies"
    db.c.execute(drop_table_statement)


@pytest.fixture(scope='module')
def movies():
    """ Setup of the example data for the tests"""

    movies = DataDownload(titles_to_get='titles.txt')
    movies.read_titles()
    results = movies.get_titles_using_api()

    yield results


def test_db_create(database):
    """
    Testing creating table in the db
    :param database:
    """

    # Checking exception if tables already exist
    with pytest.raises(sqlite3.OperationalError) as exec_info:
        database.db_create()
    print(exec_info.value)


def test_insert_data(movies, database):
    """
    Testing inserting data
    :param database:
    :param movies:
    """

    # Inserting some data once
    results = movies
    database.insert_data(results)

    # Test for inserting duplicates exception
    with pytest.raises(sqlite3.IntegrityError) as exec_info:
        database.insert_data(results)  # Inserting some data for the second time
    print(exec_info.value)


def test_get_data_by_title(database):
    """
    Testing selecting data by title
    :param database:
    """

    # Example query
    results = database.get_data_by_title('Kac Wawa')

    assert len(results) == 1  # Only one result
    assert results[0]['Title'] == 'Kac Wawa'  # Data matches expected

    # Fail case
    results = database.get_data_by_title('KacWawa')  # Query by not existing title
    assert not results  # Expecting empty list of results


def test_get_data_sort_by(database):
    """
    Testing sorting data by parameter
    :param database:
    """

    # Query by year
    results = database.get_data_sort_by('year')

    assert results[0]['Title'] == '1917'  # Data matches expected
    assert results[1]['Title'] == 'Joker'  # Data matches expected
    assert results[2]['Title'] == 'Kac Wawa'  # Data matches expected
    assert len(results) == 3  # 3 results as expected

    # Query by metascore
    results = database.get_data_sort_by('metascore')

    assert results[0]['Title'] == '1917'  # Data matches expected
    assert results[1]['Title'] == 'Joker'  # Data matches expected
    assert len(results) == 2  # 2 results as expected
