import sqlite3

import pytest
from movies_db import DBConfig, DataInsert


@pytest.fixture(scope='module')
def database():
    """ Setup of the database before tests """

    db = DBConfig(db_name='resources/movies_test.sqlite')  # Creating new db
    with db.conn:
        try:
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
        except sqlite3.IntegrityError as e:
            raise e

    yield db

    # Teardown - deleting the tested table
    db.c.execute("DROP TABLE MOVIES")


@pytest.fixture(scope='module')
def data_insert(database):
    """
    Setup the data insert class before tests
    :param database:
    :return:
    """

    # Setup
    data_insert = DataInsert(db=database)
    yield data_insert

    # Teardown
    del data_insert


@pytest.fixture(scope='module')
def titles_to_add(database):
    """
    Setup the titles to insert insert before tests
    :param database:
    :return:
    """

    # Setup
    titles_to_add = ['Memento', 'In Bruges', 5649]
    yield titles_to_add

    # Teardown
    del titles_to_add


def test_handle(data_insert, titles_to_add, database):
    """
    Testing the handle method
    :param database:
    :param data_insert:
    :param titles_to_add:
    :return:
    """

    # Testing corectness of the output
    result = data_insert.handle(parameter=titles_to_add)
    assert result == 'All titles successfully added!'

    # Testing if titles are added to the db
    sql_statement = f"SELECT * FROM {database.movies_table}"
    results = database.execute_statement(sql_statement)

    assert results[0]['Title'] == 'Memento'
    assert results[1]['Title'] == 'In Bruges'
    assert results[2]['Title'] == '5649'

    # Fail case - inserting duplicate
    with pytest.raises(sqlite3.IntegrityError) as exec_info:
        result = data_insert.handle(parameter='Memento')
    print(exec_info.value)

