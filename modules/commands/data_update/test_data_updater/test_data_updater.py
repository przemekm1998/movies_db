import concurrent
import json
import sqlite3
from pprint import pprint as pp

import pytest

from modules.commands.data_update.data_update import DataUpdater
from modules.db_config.db_config import DBConfig


@pytest.fixture(scope='module')
def database():
    """ Setup of the database before tests """

    db = DBConfig(
        db_name='/home/przemek/PycharmProjects/movies_db/modules/commands/data_update/test_data_updater/resources/movies_test.sqlite')  # Creating new db
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
            db.c.execute(
                """INSERT INTO MOVIES(TITLE) VALUES ('The Shawshank Redemption')""")
            db.c.execute("""INSERT INTO MOVIES(TITLE) VALUES ('Memento')""")
            db.c.execute("""INSERT INTO MOVIES(TITLE) VALUES ('In Bruges')""")
            db.c.execute("""INSERT INTO MOVIES(TITLE) VALUES ('Gods')""")
            db.c.execute("""INSERT INTO MOVIES(TITLE) VALUES ('The Godfather')""")
            db.c.execute("""INSERT INTO MOVIES(TITLE) VALUES ('Niemategonapewno')""")
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
    with concurrent.futures.ThreadPoolExecutor() as executor:
        downloaded_data = executor.map(data_updater.download_data, empty_titles)

    yield downloaded_data

    # Teardown
    del downloaded_data


def test_empty_titles(empty_titles):
    """
    Testing getting the list of empty titles
    :param empty_titles:
    :return:
    """

    # Testing the results of getting list of empty titles
    assert empty_titles[0]['Title'] == 'The Shawshank Redemption'
    assert empty_titles[1]['Title'] == 'Memento'
    assert empty_titles[2]['Title'] == 'In Bruges'
    assert empty_titles[3]['Title'] == 'Gods'
    assert empty_titles[4]['Title'] == 'The Godfather'


def test_download_data(downloaded_data):
    """
    Testing downloading the data using API
    :param downloaded_data:
    :return:
    """

    # Checking json data corectness
    # Ignore assertion error if imdbVotes doesn't match
    with open(
            '/home/przemek/PycharmProjects/movies_db/modules/commands/data_update/test_data_updater/resources/json_movies.json',
            'r') as correct_file:
        correct_json = json.load(correct_file)
        assert correct_json == next(downloaded_data)[0]


def test_update_data(data_updater, downloaded_data):
    """
    Test inserting the data to db
    :param data_updater:
    :param downloaded_data:
    :return:
    """
    results = data_updater.update_data(downloaded_data)
    assert results == [{'Title': 'Memento', 'Status': 'Updated'},
                       {'Title': 'In Bruges', 'Status': 'Updated'},
                       {'Title': 'Gods', 'Status': 'Updated'},
                       {'Title': 'The Godfather', 'Status': 'Updated'},
                       {'Error': 'Movie not found!', 'Response': 'False'}]
