import json
import sqlite3

import pytest
from movies_db import DataUpdater, DBConfig


@pytest.fixture(scope='module')
def database():
    """ Setup of the database before tests """

    db = DBConfig(db_name='resources/movies_test.sqlite')  # Creating new db
    with db.conn:
        db.c.execute("""CREATE TABLE IF NOT EXISTS MOVIES (
                    ID INTEGER PRIMARY KEY,
                    TITLE text,
                    YEAR integer, 
                    RUNTIME text, 
                    GENRE text, 
                    DIRECTOR text, 
                    CAST text, 
                    WRITER text, 
                    LANGUAGE text, 
                    COUNTRY text, 
                    AWARDS text, 
                    IMDb_Rating float, 
                    IMDb_votes integer, 
                    BOX_OFFICE integer,
                    UNIQUE(TITLE));
                    """)

        try:
            db.c.execute("""INSERT INTO MOVIES(TITLE) VALUES ('The Shawshank Redemption')""")
            db.c.execute("""INSERT INTO MOVIES(TITLE) VALUES ('Memento')""")
            db.c.execute("""INSERT INTO MOVIES(TITLE) VALUES ('In Bruges')""")
            db.c.execute("""INSERT INTO MOVIES(TITLE) VALUES ('Gods')""")
            db.c.execute("""INSERT INTO MOVIES(TITLE) VALUES ('The Godfather')""")
        except sqlite3.IntegrityError:
            pass

    yield db

    # Teardown - deleting the tested table
    db.c.execute("DROP TABLE MOVIES")


@pytest.fixture(scope='module')
def data_updater(database):
    """
    Setup the data updater class before tests
    :param database:
    :return:
    """

    # Setup
    data_updater = DataUpdater(db=database)
    yield data_updater

    # Teardown
    del data_updater


@pytest.fixture(scope='module')
def empty_titles(data_updater, database):
    """
    Setup the empty titles before tests
    :param data_updater:
    :param database:
    :return:
    """

    # Setup
    empty_titles = database.execute_statement(data_updater.sql_empty_titles_statement)
    yield empty_titles

    # Teardown
    del empty_titles


@pytest.fixture(scope='module')
def downloaded_data(data_updater, empty_titles):
    """
    Setup the downloaded data before tests
    :param empty_titles:
    :param data_updater:
    :return:
    """

    # Setup
    downloaded_data = data_updater.download_data(empty_titles)
    yield downloaded_data

    # Teardown
    del downloaded_data


@pytest.mark.parametrize('index, title',
                         [
                             (0, 'The Shawshank Redemption'),
                             (1, 'Memento'),
                             (2, 'In Bruges'),
                             (3, 'Gods'),
                             (4, 'The Godfather')
                         ])
def test_empty_titles(empty_titles, index, title):
    """
    Testing getting the list of empty titles
    :param title:
    :param index:
    :param empty_titles:
    :return:
    """

    # Testing the results of getting list of empty titles
    assert empty_titles[index]['Title'] == title


def test_download_data(downloaded_data):
    """
    Testing downloading the data using API
    :param downloaded_data:
    :return:
    """

    # Checking json data corectness
    with open('resources/json_movies.json', 'r') as correct_file:
        correct_json = json.load(correct_file)

    assert correct_json == downloaded_data[0]