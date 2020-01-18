import _sqlite3

import pytest
from movies_download import DBConfig, DataDownload


@pytest.fixture(scope='module')
def database():
    """ Setup before tests """

    # Setup
    db = DBConfig(db_name='movies.db')
    db.db_create()

    yield db

    # Teardown
    # drop_table_statement = "DROP TABLE movies"
    # db.c.execute(drop_table_statement)


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
    database.insert_data(results)
