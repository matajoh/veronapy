"""Region tests."""

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
