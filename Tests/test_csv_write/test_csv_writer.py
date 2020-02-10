import sqlite3

import pytest
from movies_db import DBConfig, DataDownload, CSVWriter


@pytest.fixture(scope='module')
def movies():
    """ Setup of the example data for the tests"""

    movies = DataDownload(titles_to_get='titles.txt')
    movies.read_titles()
    results = movies.get_titles_using_api()

    yield results


@pytest.fixture(scope='module')
def database(movies):
    """ Setup of the database before tests """

    # Setup - creating new database
    db = DBConfig(db_name='movies.db')
    db.db_create()
    db.insert_data(movies)

    yield db

    # Teardown - deleting the tested table
    drop_table_statement = "DROP TABLE movies"
    db.c.execute(drop_table_statement)


def test_write_csv(database):
    """
    Saving query to csv file
    :param database:
    :return:
    """

    # Fail case - not existing parameter
    with pytest.raises(sqlite3.OperationalError) as exec_info:
        results = database.sort_data(parameter='metascorefajne')  # Not existing param metascorefajne
    print(exec_info.value)

    # Faile case - empty result
    # with pytest.raises(IndexError) as exec_info:
    #     results = database.filter_data(parameter='actors', value='KacWawa')
    #     CSVWriter.write_csv(title='test_metascore.csv', data=results)
    # print(exec_info.value)

    # Single result
    results = database.filter_data(parameter='director', value='Todd Phillips')
    CSVWriter.write_csv(title='test_metascore.csv', data=results)

    # Multiple results
    # results = database.sort_data(parameter='metascorefajne')
    # CSVWriter.write_csv(title='test_metascore.csv', data=results)
