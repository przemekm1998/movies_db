import sqlite3

import pytest
from movies_db import DBConfig, CSVWriter


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
    # db.c.execute("DROP TABLE MOVIES")


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


def test_write_csv(database, downloaded_data):
    """
    Saving query to csv file
    :param downloaded_data:
    :param database:
    :return:
    """

    database.update_data(downloaded_data)

    # Fail case - not existing parameter
    with pytest.raises(sqlite3.OperationalError) as exec_info:
        results = database.sort_data(parameter='metascore')  # Not existing param metascore
    print(exec_info.value)

    # Faile case - empty result
    with pytest.raises(IndexError) as exec_info:
        results = database.filter_data(parameter='cast', value='Borys Szyc')
        CSVWriter.write_csv(title='results/test_metascore.csv', data=results)
    print(exec_info.value)

    # Single result
    results = database.filter_data(parameter='language', value='English')
    CSVWriter.write_csv(title='results/test_english.csv', data=results)

    # Multiple results
    results = database.sort_data(parameter='imdb_rating')
    CSVWriter.write_csv(title='results/test_imdbRating.csv', data=results)
