from veronapy import region, wait, when


region = region()
with region as r:
    r.a = 0
    print("a", r.a)

region.make_shareable()
print(region)


@when(region)
def _(r):
    r.a = 1
    print("a", r.a)


@when(region)
def _(r):
    r.a += 1
    print("a", r.a)


wait()
