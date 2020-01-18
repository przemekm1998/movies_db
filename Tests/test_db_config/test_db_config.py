import pytest
from movies_db import DBConfig, DataDownload


@pytest.fixture(scope='module')
def database():
    """ Setup before tests """

    # Setup
    db = DBConfig(db_name='movies.db')
    db.db_create()

    yield db

    # Teardown
    drop_table_statement = "DROP TABLE movies"
    db.c.execute(drop_table_statement)


@pytest.fixture(scope='module')
def movies():
    """ Setup """

    movies = DataDownload(titles_to_get='titles.txt')
    movies.read_titles()
    results = movies.get_titles_using_api()
    yield results


def test_db_create(database):
    """
    Testing creating table in the db
    :param database:
    """

    # Checking exception if tables already exist
    database.db_create()


def test_insert_data(movies, database):
    """
    Testing inserting data
    :param database:
    :param movies:
    """

    results = movies
    database.insert_data(results)

    # Test for inserting duplicates
    database.insert_data(results)


def test_get_data_by_title(database):
    """
    Testing selecting data by title
    :param database:
    """

    results = database.get_data_by_title('Kac Wawa')
    not_true = database.get_data_by_title('whatever')

    correct_result = [('Kac Wawa', 2012, 'N/A', '02 Mar 2012', '97 min', 'Comedy', 'Lukasz Karwowski',
                       'Piotr Czaja, Jacek Samojlowicz, Krzysztof Weglarz',
                       'Borys Szyc, Sonia Bohosiewicz, Roma Gasiorowska, Miroslaw Zbrojewicz',
                       'Five buddies take part in a crazy bachelor party of one of them. Meanwhile, the bride and her female friends have a nice time with male strippers. The parties get totally wild and out of control.',
                       'Polish', 'Poland', 'N/A',
                       'https://m.media-amazon.com/images/M/MV5BMWVlNjcxYmEtMmYyYy00ZjAwLTk0MWQtY2RhNGM1YmIzMjIxXkEyXkFqcGdeQXVyMTc4MzI2NQ@@._V1_SX300.jpg',
                       'N/A', 1.4, 675, 'tt2282829', 'movie', 'N/A', 'N/A', 'N/A', 'N/A')]

    assert results == correct_result
    assert len(not_true) == 0


def test_get_data_sort_by(database):
    """
    Testing sorting data by parameter
    :param database:
    """

    # Query by year
    results = database.get_data_sort_by('year')

    assert results[0][0] == '1917'
    assert results[1][0] == 'Joker'
    assert results[2][0] == 'Kac Wawa'
    assert len(results) == 3

    # Query by metascore
    results = database.get_data_sort_by('metascore')

    assert results[0][0] == '1917'
    assert results[1][0] == 'Joker'
    assert len(results) == 2

