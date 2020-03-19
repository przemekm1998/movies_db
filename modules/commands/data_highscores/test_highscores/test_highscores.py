import pytest

from movies_db import DBConfig, DataHighscores


@pytest.fixture(scope='module')
def data_highscores():
    """ Setup of the data highscores class before tests """

    db = DBConfig(db_name='resources/movies_test.sqlite')  # Creating new db connection
    data_highscores = DataHighscores(db=db)

    yield data_highscores


def test_handle(data_highscores):
    """
    Testing highscores
    :param data_highscores:
    """

    # Filter by title
    results = data_highscores.handle(parameter=True)

    assert results[0]['col_name'] == 'RUNTIME'
    assert results[0]['max_val'] == '96 min'
    assert results[0]['Title'] == '12 Angry Men'

    assert results[1]['col_name'] == 'IMDb_Rating'
    assert results[1]['max_val'] == 9.3
    assert results[1]['Title'] == 'The Shawshank Redemption'

    assert results[2]['col_name'] == 'BOX_OFFICE'
    assert results[2]['max_val'] == 533316061
    assert results[2]['Title'] == 'The Dark Knight'

    assert results[3]['col_name'] == 'IMDb_votes'
    assert results[3]['max_val'] == 2188727
    assert results[3]['Title'] == 'The Shawshank Redemption'
