"""Example showing merge sort using BoC."""

import random

from veronapy import region, wait, when


Threshold = 10


def sort_section(source: tuple, start: int, end: int, output: region):
    if end < start:
        return

    if end - start + 1 <= Threshold:
        # when output:
        @when(output)
        def _(output):
            print("sorting", start, end)
            output.values = list(sorted(source[start:end + 1]))

        return

    lhs = region("lhs").make_shareable()
    rhs = region("rhs").make_shareable()
    mid = (start + end) // 2
    sort_section(source, start, mid, lhs)
    sort_section(source, mid + 1, end, rhs)

    # when output:
    @when(output, lhs, rhs)
    def _(output, lhs, rhs):
        print("merging", start, end)

        i = 0
        j = 0
        values = []
        while i < len(lhs.values) and j < len(rhs.values):
            if lhs.values[i] < rhs.values[j]:
                values.append(lhs.values[i])
                i += 1
            else:
                values.append(rhs.values[j])
                j += 1

        while i < len(lhs.values):
            values.append(lhs.values[i])
            i += 1

        while j < len(rhs.values):
            values.append(rhs.values[j])
            j += 1

        output.values = values


def main():
    # Create an immutable list of integers as input
    values = tuple([random.randint(0, 1000) for _ in range(1000)])
    print("unsorted:", values)

    # Create a region to hold the output
    output = region("MergeSort").make_shareable()

    # Sort the list
    sort_section(values, 0, len(values) - 1, output)

    # when r:
    @when(output)
    def _(output):
        for i in range(len(output.values) - 1):
            assert output.values[i] <= output.values[i + 1]
        
        print("sorted!")


if __name__ == "__main__":
    main()
    wait()
