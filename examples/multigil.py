import os
import time
from veronapy import region, when, wait


def main():
    """Main function of the script."""

    try:
        num_regions = len(os.sched_getaffinity(0))
    except AttributeError:
        num_regions = os.cpu_count()

    print("Running {} when statements in parallel".format(num_regions))
    regions = [region("r" + str(i)) for i in range(num_regions)]
    for r in regions:
        with r:
            r.count = 0

        r.make_shareable()

    for r in regions:
        @when(r)
        def _(r):
            r.start = time.time()
            for i in range(100000):
                r.val = pow(i, 7)

            r.end = time.time()

    @when(*regions)
    def _(*regions):
        jobs = [tuple([r.start, r.end, r.name]) for r in regions]
        jobs.sort()
        start = jobs[0][0]
        for job in jobs:
            print("Region {}: {:.2f} - {:.2f} ({:.2f}s)".format(job[2], job[0] - start, job[1] - start, job[1] - job[0]))


if __name__ == "__main__":
    main()
    wait()
