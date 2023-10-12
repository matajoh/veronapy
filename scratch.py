from veronapy import region, wait, when, run


def test_when():
    run()
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

    wait()


def test_detach():
    run()
    c1 = region("c1")
    c2 = region("c2")

    with c1, c2:
        c1.a = "foo"
        c2.b = "bar"

    c1.make_shareable()
    c2.make_shareable()

    # when c1, c2:
    @when(c1, c2)
    def _(c1, c2):
        r1, r2 = c1.detach_all("r1"), c2.detach_all("r2")
        merge = c1.merge(r2)
        c1.b = merge.b
        merge = c2.merge(r1)
        c2.a = merge.a

    # when c1, c2:
    @when(c1, c2)
    def _(c1, c2):
        assert c1.b == "bar"
        assert c2.a == "foo"

    wait()


if __name__ == "__main__":
    test_when()
    print("PASSED")
    test_detach()
