import sqlite3

import pytest
from movies_db import DBConfig, DataDownload, CSVWriter


@pytest.fixture(scope='module')
def database():
    """ Setup before tests """

    # Setup
    movies = DataDownload(titles_to_get='titles.txt')
    movies.read_titles()
    results = movies.get_titles_using_api()

    db = DBConfig(db_name='movies.db')
    db.db_create()
    db.insert_data(results)

    yield db

    # Teardown
    drop_table_statement = "DROP TABLE movies"
    db.c.execute(drop_table_statement)


def test_write_csv(database):
    """
    Saving query to csv file
    :param database:
    :return:
    """

    # Fail case
    with pytest.raises(sqlite3.OperationalError) as exec_info:
        results = database.get_data_sort_by('metascorefajne')
        CSVWriter.write_csv(title='test_metascore_fail.csv', data=results)
    print(exec_info.value)

    # Multiple results
    results = database.get_data_sort_by('metascore')
    CSVWriter.write_csv(title='test_metascore.csv', data=results)

    # Single result
    results = database.get_data_by_title('Kac Wawa')
    CSVWriter.write_csv(title='test_metascore.csv', data=results)
