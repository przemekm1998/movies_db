import sqlite3

import pytest
from movies_db import DBConfig, DataDelete


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
            db.c.execute(
                """INSERT INTO MOVIES(TITLE) VALUES ('The Shawshank Redemption')""")
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
def data_delete(database):
    """
    Setup the data insert class before tests
    :param database:
    :return:
    """

    # Setup
    data_insert = DataDelete(db=database)
    yield data_insert

    # Teardown
    del data_insert


@pytest.fixture(scope='module')
def titles_to_delete(database):
    """
    Setup the titles to insert insert before tests
    :param database:
    :return:
    """

    # Setup
    titles_to_delete = ['Memento', 'In Bruges']
    yield titles_to_delete

    # Teardown
    del titles_to_delete


def test_handle(data_delete, titles_to_delete, database):
    """
    Testing the handle method
    :param titles_to_delete:
    :param data_delete:
    :param database:
    :return:
    """

    # Testing corectness of the output
    result = data_delete.handle(parameter=titles_to_delete)
    assert result == [{'Status': 'Deleted', 'Title': 'Memento'},
                      {'Status': 'Deleted', 'Title': 'In Bruges'}]

    # Testing if titles are deleted from the db
    sql_statement = f"SELECT * FROM {database.movies_table}"
    results = database.execute_statement(sql_statement)

    results = [result['Title'] for result in results]

    assert titles_to_delete not in results
