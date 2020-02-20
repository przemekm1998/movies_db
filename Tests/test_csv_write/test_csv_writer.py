import sqlite3

import pytest
from movies_db import DBConfig, CSVWriter, DataFilter, DataSorter, DataUpdater


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
def csv_writer(database):
    """
    Setup of the CSV Writer class
    :param database:
    :return:
    """

    csv_writer = CSVWriter()

    yield csv_writer


@pytest.fixture(scope='module')
def data_sorter(database):
    """
    Setup of the data sorter class
    :param database:
    :return:
    """

    data_sorter = DataSorter(database)

    yield data_sorter


@pytest.fixture(scope='module')
def data_filter(database):
    """
    Setup of the data sorter class
    :param database:
    :return:
    """

    data_filter = DataFilter(database)

    yield data_filter


@pytest.fixture(scope='module')
def data_update(database):
    """
    Setup of the data sorter class
    :param database:
    :return:
    """

    data_update = DataUpdater(database)

    yield data_update


def test_create_title(csv_writer, data_sorter, data_filter):
    """
    Testing creating the title
    :param data_filter:
    :param data_sorter:
    :param csv_writer:
    :return:
    """

    keyword = data_sorter.get_keyword()
    title = csv_writer.create_title(keyword)
    assert keyword in title

    keyword = data_filter.get_keyword()
    title = csv_writer.create_title(keyword)
    assert keyword in title


def test_write_csv(data_sorter, data_filter, csv_writer, data_update):
    """
    Saving query to csv file
    :param data_update:
    :param csv_writer:
    :param data_filter:
    :param data_sorter:
    :return:
    """

    sorted_data = data_sorter.handle(parameter='box_office')
    csv_writer.write_csv(keyword=data_sorter.get_keyword(), data=sorted_data)

    filtered_data = data_filter.handle(parameter=['language', 'english'])
    csv_writer.write_csv(keyword=data_filter.get_keyword(), data=filtered_data)

    # Empty results case
    with pytest.raises(IndexError) as exec_info:
        updated_data = data_update.handle(parameter=True)
        csv_writer.write_csv(keyword=data_update.get_keyword(), data=updated_data)
    print(exec_info.value)
