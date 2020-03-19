import pytest
from movies_db import CLInterface


@pytest.fixture(scope='module')
def test_get_args():
    """ Getting empty titles from db for all tests """

    interface = CLInterface().get_args()  # Getting list of titles

    yield interface
