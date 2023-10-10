
class Merge:
    """An object that contains references to the objects merged into a region."""


class RegionIsolationError(Exception):
    """Error raised for issues related to region isolation."""


class region:
    """An object that reifies a region and permits manipulations of the entire region."""

    def __init__(self, name: str = None):
        """Constructor."""

    @property
    def is_shared(self) -> bool:
        """Returns whether the region is shared."""

    @property
    def is_open(self) -> bool:
        """Returns whether the region is open."""

    def make_shareable(self) -> "region":
        """Makes the region shareable."""

    def merge(self, other: "region") -> Merge:
        """Merges the other region into this one.

        This function will raise an error if this region is not open.
        The end result is that all objects and regions in the other region
        will be in this one, and the other region will be empty.

        Returns:
            a Merge object that contains references to the objects
            merged into this region.
        """


def when(*regions: region):
    """Returns a decorator that schedules work to be done when the regions are open."""
