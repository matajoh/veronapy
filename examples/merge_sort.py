"""Example showing merge sort using BoC."""

import random
import threading
import time

from veronapy import region, wait, when


Threshold = 10


class Item:
    def __init__(self, value):
        self.value = value

    def __lt__(self, other):
        return self.value < other.value

    def __le__(self, other):
        return self.value <= other.value

    def __repr__(self):
        return str(self.value)

    def __str__(self):
        return str(self.value)


def sort_section(source: tuple, start: int, end: int, output: region):
    if end < start:
        return

    if end - start + 1 <= Threshold:
        unsorted = region("unsorted_{}_{}".format(start, end))
        with unsorted:
            unsorted.start = start
            unsorted.end = end
            unsorted.values = list(source[start:end + 1])
        
        unsorted.make_shareable()

        # when output:
        @when(unsorted, output)
        def _(unsorted, output):
            unsorted.values.sort()
            merge = output.merge(unsorted)
            output.values = merge.values

        return

    mid = (start + end) // 2
    lhs = region("lhs_{}_{}".format(start, mid))
    rhs = region("rhs_{}_{}".format(mid + 1, end))

    with lhs:
        lhs.start = start
        lhs.end = mid
        lhs.values = []

    with rhs:
        rhs.start = mid + 1
        rhs.end = end
        rhs.values = []

    sort_section(source, start, mid, lhs.make_shareable())
    sort_section(source, mid + 1, end, rhs.make_shareable())

    # when output:
    @when(output, lhs, rhs)
    def _(output, lhs, rhs):
        lhs = output.merge(lhs)
        rhs = output.merge(rhs)
        i = 0
        j = 0
        values = []
        lhs_len = lhs.end - lhs.start + 1
        rhs_len = rhs.end - rhs.start + 1
        while i < lhs_len and j < rhs_len:
            if lhs.values[i] < rhs.values[j]:
                values.append(lhs.values[i])
                i += 1
            else:
                values.append(rhs.values[j])
                j += 1

        while i < lhs_len:
            values.append(lhs.values[i])
            i += 1

        while j < rhs_len:
            values.append(rhs.values[j])
            j += 1

        output.values = values


def main():
    # Create an immutable list of integers as input
    values = tuple([Item(random.randint(0, 100)) for _ in range(100)])
    print("unsorted:", values)

    # Create a region to hold the output
    output = region("MergeSort")

    with output:
        output.start = 0
        output.end = len(values) - 1
        output.values = []

    output.make_shareable()

    # Sort the list
    sort_section(values, 0, len(values) - 1, output)

    print("All work queued")

    # when r:
    @when(output)
    def _(output):
        print("checking if sorted...")
        for i in range(len(output.values) - 1):
            assert output.values[i] <= output.values[i + 1]

        print("sorted!")


if __name__ == "__main__":
    start = time.time()
    main()
    wait()
    elapsed = time.time() - start
    print("time taken:", elapsed)
