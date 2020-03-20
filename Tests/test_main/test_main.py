import sqlite3

import pytest

from modules.commands.data_compare.data_compare import DataCompare
from modules.commands.data_delete.data_delete import DataDelete
from modules.commands.data_filter.data_filter import DataFilter
from modules.commands.data_insert.data_insert import DataInsert
from modules.commands.data_sort.data_sort import DataSorter
from modules.commands.data_update.data_update import DataUpdater
from modules.db_config.db_config import DBConfig
from movies_db import Main


@pytest.fixture(scope='module')
def database():
    """ Setup of the database before tests """

    db = DBConfig(db_name='/home/przemek/PycharmProjects/movies_db/Tests/test_main/resources/movies_test.sqlite')  # Creating new db
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
    commands['write_csv'] = False

    yield commands


@pytest.fixture(scope='module')
def handlers(database):
    """ Fixture of handlers to test """

    handlers = [DataUpdater(database), DataSorter(database), DataFilter(database),
                DataCompare(database), DataInsert(database), DataDelete(database)]

    yield handlers


def test_handle_commands(commands_working, handlers):
    from timeit import default_timer as timer
    from datetime import timedelta

    start = timer()
    Main.handle_commands(commands_working, handlers)

    end = timer()
    print(timedelta(seconds=end - start))

