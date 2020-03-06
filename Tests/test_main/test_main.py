import sqlite3

import pytest
from movies_db import Main, DataUpdater, DBConfig, DataSorter, DataFilter, DataCompare, DataInsert, DataDelete


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
def commands_working():
    """ Fixture of commands to test """

    commands = dict()
    commands['update'] = 'True'
    commands['sort_by'] = 'Director'
    commands['filter_by'] = ['language', 'English']
    commands['compare'] = ['box_office', 'Memento', 'The Shawshank Redemption']
    commands['insert'] = ['Kac Wawa', '1917']

    yield commands


@pytest.fixture(scope='module')
def handlers(database):
    """ Fixture of handlers to test """

    handlers = [DataUpdater(database), DataSorter(database), DataFilter(database),
                DataCompare(database), DataInsert(database), DataDelete(database)]

    yield handlers


def test_handle_commands(commands_working, handlers):
    Main.handle_commands(commands_working, handlers)
