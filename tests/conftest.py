import pytest

from veronapy import run, wait


@pytest.fixture(scope="session", autouse=True)
def vpy_wrapper():
    run()
    yield
    wait()


def vpy_run(callable):
    run()
    try:
        callable()
    finally:
        wait()
