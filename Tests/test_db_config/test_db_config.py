import sqlite3

import pytest
from movies_db import DBConfig, DataDownload, CSVWriter


@pytest.fixture(scope='module')
def database():
    """ Setup before tests """

    # Setup
    db = DBConfig(db_name='movies.db')
    db.db_create()

    yield db

    # Teardown
    drop_table_statement = "DROP TABLE movies"
    db.c.execute(drop_table_statement)


@pytest.fixture(scope='module')
def movies():
    """ Setup """

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
    database.db_create()


def test_insert_data(movies, database):
    """
    Testing inserting data
    :param database:
    :param movies:
    """

    results = movies
    database.insert_data(results)

    # Test for inserting duplicates


def test_get_data_by_title(database):
    """
    Testing selecting data by title
    :param database:
    """

    results = database.get_data_by_title('Kac Wawa')

    assert len(results) == 1
    assert results[0]['Title'] == 'Kac Wawa'

    with pytest.raises(ValueError) as exec_info:
        results = database.get_data_by_title('KacWawa')
    print(exec_info.value)


def test_get_data_sort_by(database):
    """
    Testing sorting data by parameter
    :param database:
    """

    # Query by year
    results = database.get_data_sort_by('year')

    assert results[0]['Title'] == '1917'
    assert results[1]['Title'] == 'Joker'
    assert results[2]['Title'] == 'Kac Wawa'
    assert len(results) == 3

    # Query by metascore
    results = database.get_data_sort_by('metascore')

    assert results[0]['Title'] == '1917'
    assert results[1]['Title'] == 'Joker'
    assert len(results) == 2
