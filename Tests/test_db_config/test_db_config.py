import json
import sqlite3

import pytest
from movies_db import DBConfig


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
def titles(database):
    """ Getting empty titles from db for all tests """

    results = database.get_empty_titles()  # Getting list of titles

    yield results


@pytest.fixture(scope='module')
def downloaded_data(database, titles):
    """ Downloading data once for every test needed """

    result_json = database.download_data(titles)

    yield result_json


def test_get_titles(titles):
    """
    Test fetching titles with empty data
    :param titles:
    :return:
    """

    # List properties
    assert type(titles) is list
    assert titles is not None
    assert len(titles) == 5

    # List content
    assert 'The Shawshank Redemption' in titles[0]
    assert 'Memento' in titles[1]
    assert 'The Godfather' in titles[4]


def test_download_data():
    """
    Testing the json formatting
    """

    # Creating new database with 1 record just to test json formatting
    database = DBConfig(db_name='resources/json_movies.sqlite')

    # Fetching empty title
    results = database.get_empty_titles()

    # Downloading data using api in json format
    result_json = database.download_data(results)

    # Loading the properly formatted json fixture
    with open('resources/json_movies.json', 'r') as correct_file:
        correct_json = json.load(correct_file)

    assert correct_json == result_json[0]


def test_update_data(database, downloaded_data):
    """
    Testing updating data
    """

    # Updating table with downloaded data
    database.update_data(downloaded_data)

    # Fetching rows with updated data
    database.c.execute(f"SELECT * FROM MOVIES;")
    database_data = database.c.fetchall()

    assert database_data is not None
    assert len(database_data) == 5  # Expected 5 results

    # Comparing downloaded data by API with inserted data
    assert database_data[0]['Director'] == downloaded_data[0]['Director']
    assert database_data[1]['Cast'] == downloaded_data[1]['Actors']
    assert database_data[3]['Runtime'] == downloaded_data[3]['Runtime']
    assert database_data[4]['imdb_Rating'] == float(downloaded_data[4]['imdbRating'])


def test_filter_data(database):
    """
    Testing filtering data
    :param database:
    """

    # Filter by title
    results = database.filter_data(parameter='title', value='The Godfather')
    assert len(results) == 1  # Only one result expected
    assert results[0]['Title'] == 'The Godfather'  # Data matches expected

    # Filter by language
    results = database.filter_data(parameter='language', value='English')

    assert len(results) == 4  # 4 results expected
    assert results[0]['Title'] == 'The Shawshank Redemption'  # Data matches expected
    assert results[1]['Title'] == 'Memento'  # Data matches expected
    assert results[2]['Title'] == 'In Bruges'  # Data matches expected
    assert results[3]['Title'] == 'The Godfather'  # Data matches expected

    # Fail case filtering
    with pytest.raises(IndexError) as exec_info:
        results = database.filter_data(parameter='year', value='3000')  # No movies made in year 3000
    print(exec_info.value)

    # Fail case - query by column which doesn't exist
    with pytest.raises(sqlite3.OperationalError) as exec_info:
        results = database.filter_data(parameter='metascore', value='5')
    print(exec_info.value)

    # Filter by cast
    results = database.filter_data(parameter='cast', value='Tim Robbins')

    assert results is not None
    assert len(results) == 1

    assert 'Tim Robbins' in results[0]['Cast']
    assert results[0]['Title'] == 'The Shawshank Redemption'


def test_get_data_sort_by(database):
    """
    Testing sorting data by parameter
    :param database:
    """

    # Query by year
    results = database.sort_data('year')
    assert results[0]['Title'] == 'Gods'
    assert results[1]['Title'] == 'In Bruges'
    assert results[2]['Title'] == 'Memento'
    assert results[3]['Title'] == 'The Shawshank Redemption'
    assert results[4]['Title'] == 'The Godfather'

    # Query by imdb rating
    results = database.sort_data('imdb_rating')
    assert results[0]['Title'] == 'The Shawshank Redemption'
    assert results[1]['Title'] == 'The Godfather'
    assert results[2]['Title'] == 'Memento'
    assert results[3]['Title'] == 'In Bruges'
    assert results[4]['Title'] == 'Gods'

    # Query by box office
    results = database.sort_data('box_office')

    # Fail case - query by column which doesn't exist
    with pytest.raises(sqlite3.OperationalError) as exec_info:
        results = database.sort_data('metascore')
    print(exec_info.value)


def test_compare(database):
    """
    Testing comparing data by parameter
    :param database:
    :return:
    """

    # Compare by imdb_rating
    titles_to_compare = ['Memento', 'The Godfather']
    results = database.compare(parameter='imdb_rating', titles_to_compare=titles_to_compare)
    assert results[0]['Title'] == 'The Godfather'

    # Compare by box office
    titles_to_compare = ['Memento', 'In Bruges']
    results = database.compare(parameter='box_office', titles_to_compare=titles_to_compare)
    assert results[0]['Title'] == 'Memento'

    # Compare by runtime
    titles_to_compare = ['Gods', 'The Godfather']
    results = database.compare(parameter='runtime', titles_to_compare=titles_to_compare)
    assert results[0]['Title'] == 'The Godfather'
