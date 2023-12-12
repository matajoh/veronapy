"""Region tests."""

import json

import veronapy as vp
from conftest import vpy_run


class MockObject:
    """Mock object used for testing."""

    def __str__(self) -> str:
        """Produces a simple representation of the object graph."""
        return json.dumps(self.__dict__, default=lambda o: str(o))


def test_creation():
    r0 = vp.region("a")
    r1 = vp.region("b")
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
        r0.id = 100
    except AttributeError:
        pass
    else:
        raise AssertionError("Should have raised AttributeError")

    try:
        r0.is_open = True
    except AttributeError:
        pass
    else:
        raise AssertionError("Should have raised AttributeError")

    try:
        r0.is_shared = True
    except AttributeError:
        pass
    else:
        raise AssertionError("Should have raised AttributeError")

    try:
        r1.parent = r0
    except AttributeError:
        pass
    else:
        raise AssertionError("Should have raised AttributeError")

    assert r1.name == "b"
    assert r0.id != r1.id


def test_open():
    a = vp.region("a")
    assert not a.is_open
    with a:
        assert a.is_open

    assert not a.is_open


def test_parent():
    a = vp.region("a")
    b = vp.region("b")
    with a:
        a.child = b
        assert b.parent == a
        try:
            b.parent = None
        except AttributeError:
            pass
        else:
            raise AssertionError("Should have raised AttributeError")


def test_adding():
    r = vp.region()   # closed, free and private
    o = MockObject()  # free

    with r:
        r.f = o       # o becomes owned by r

    assert o.__region__ == r


def test_ownership():
    r1 = vp.region("Bank1")
    r2 = vp.region("Bank2")
    with r1, r2:
        r1.accounts = {"Alice": 1000}
        try:
            r2.accounts = r1.accounts
        except vp.RegionIsolationError:
            # ownership exception
            pass
        else:
            raise AssertionError


def test_isolation():
    r1 = vp.region("Bank1")
    x = None
    with r1:
        r1.accounts = {"Alice": 1000}
        x = r1.accounts

    try:
        print(x["Alice"])
    except vp.RegionIsolationError:
        # the region not open
        pass
    else:
        raise AssertionError


def test_ownership_with_merging():
    r1 = vp.region()
    r2 = vp.region()

    with r1, r2:
        o1 = MockObject()      # free object
        o2 = MockObject()      # free object
        o1.f = o2              # o1 and o2 are in the same implicit region
        r1.f = o1              # o1 becomes owned by r1, as does o2
        try:
            r2.f = o2          # Throws an exception as o2 is in r1
        except vp.RegionIsolationError:
            pass
        else:
            raise AssertionError


def test_region_ownership():
    r1 = vp.region("r1")
    r2 = vp.region("r2")
    r3 = vp.region("r3")

    with r1, r2:
        r1.f = r3        # OK, r3 becomes owned by r1
        try:
            r2.f = r3    # Throws exception since r3 is already owned by r1
        except vp.RegionIsolationError:
            pass
        else:
            raise AssertionError


def test_merge():
    r1 = vp.region()
    r2 = vp.region()

    with r1:
        r1.o1 = MockObject()
        r1.o1.field = "r1"

        with r2:
            r2.o2 = MockObject()
            r2.o2.field = "r2"

        merged = r1.merge(r2)            # merge the two regions
        r1.o2 = merged.o2                # create edges
        print(r1.o2)                     # verify it exists
        assert r2.o2.__region__ == r1    # validate r2 is an alias for r1


if __name__ == "__main__":
    vpy_run(test_creation)
    vpy_run(test_open)
    vpy_run(test_parent)
    vpy_run(test_adding)
    vpy_run(test_ownership)
    vpy_run(test_isolation)
    vpy_run(test_ownership_with_merging)
    vpy_run(test_region_ownership)
    vpy_run(test_merge)
