"""Region tests."""

import json
from veronapy import region


def test_creation():
    r0 = region("a")
    r1 = region("b")
    assert r0.name == "a"
    assert not r0.is_open
    assert not r0.is_shared

    try:
        r0.name = 5
    except TypeError:
        pass
    else:
        raise AssertionError("Should have raised TypeError")

    r0.name = "c"
    assert r0.name == "c"

    try:
        r0.__identity__ = 100
    except TypeError:
        pass
    else:
        raise AssertionError("Should have raised TypeError")

    try:
        r0.is_open = True
    except TypeError:
        pass
    else:
        raise AssertionError("Should have raised TypeError")

    try:
        r0.is_shared = True
    except TypeError:
        pass
    else:
        raise AssertionError("Should have raised TypeError")

    assert r1.name == "b"
    assert r0.__identity__ != r1.__identity__


def test_open():
    a = region("a")
    assert not a.is_open
    a.__enter__()
    assert a.is_open
    a.__exit__(None, None, None)
    assert not a.is_open


class MockObject:
    """Mock object used for testing."""
    def __str__(self) -> str:
        """Produces a simple representation of the object graph."""
        return json.dumps(self.__dict__)


def test_adding():
    r = region()   # closed, free and private
    o = MockObject()  # free

    with r:
        r.f = o       # o becomes owned by r

    assert o.__region__ == r


def test_ownership():
    r1 = region("Bank1")
    r2 = region("Bank2")
    with r1, r2:
        r1.accounts = {"Alice": 1000}
        try:
            r2.accounts = r1.accounts
        except RuntimeError:
            # ownership exception
            pass
        else:
            raise AssertionError


def test_isolation():
    r1 = region("Bank1")
    x = None
    with r1:
        r1.accounts = {"Alice": 1000}
        x = r1.accounts

    try:
        print(x["Alice"])
    except RuntimeError:
        # the region not open
        pass
    else:
        raise AssertionError
