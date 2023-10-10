import json
from veronapy import region, RegionIsolationError, when


class MockObject:
    """Mock object used for testing."""
    def __str__(self) -> str:
        """Produces a simple representation of the object graph."""
        return json.dumps(self.__dict__)


def test_shareable():
    r = region().make_shareable()
    assert r.is_shared


def test_when():
    my_account = region()
    your_account = region()

    with my_account, your_account:
        my_account.balance = 100
        your_account.balance = 0

    my_account.make_shareable()
    your_account.make_shareable()

    # when my_account, your_account as m, y:
    @when(my_account, your_account)
    def _(m, y):
        m.balance, y.balance = y.balance, m.balance

    # when my_account, your_account as m, y:
    @when(my_account, your_account)
    def _(m, y):
        assert m.balance == 0
        assert y.balance == 100


def test_detach():
    c1 = region("c1")
    c2 = region("c2")

    with c1, c2:
        c1.a = "foo"
        c2.b = "bar"

    c1.make_shareable()
    c2.make_shareable()

    # when c1, c2:
    @when(c1, c2)
    def _():
        r1, r2 = c1.detach_all("r1"), c2.detach_all("r2")
        merge = c1.merge(r2)
        c1.b = merge.b
        merge = c2.merge(r1)
        c2.a = merge.a

    # when c1, c2:
    @when(c1, c2)
    def _():
        assert c1.b == "bar"
        assert c2.a == "foo"


def test_when_private():
    r1 = region("Bank1")
    r2 = region("Bank2")

    try:
        # when r1
        @when(r1)
        def _():
            r1.nested_bank = r2
    except RegionIsolationError:
        # private region needs with
        pass
    else:
        raise AssertionError
