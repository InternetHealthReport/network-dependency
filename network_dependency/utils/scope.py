import logging


class Scope:
    def __init__(self, as_: int):
        self.as_ = as_
        self.as_dependencies = set()
        self.hegemony_scores = dict()

    def add_as(self, as_: int, score: float) -> None:
        # The scope contains itself with a value of 1.0, but we can
        # ignore that here.
        if as_ == self.as_:
            return
        if as_ in self.hegemony_scores:
            logging.error('Trying to add AS {} with score {} to scope {}, ' +
                          'which already contains the AS with score {}'
                          .format(as_, score, self.as_,
                                  self.hegemony_scores[as_]))
            return
        self.as_dependencies.add(as_)
        self.hegemony_scores[as_] = score

    def get_score(self, as_) -> float:
        if as_ not in self.hegemony_scores:
            logging.error('Trying to get score for AS {} which is not ' +
                          'contained in scope {}'.format(as_, self.as_))
            return -1
        return self.hegemony_scores[as_]

    def not_in(self, other) -> set:
        """Return all ASs that are contained in self but not in other."""
        return self.as_dependencies - other.as_dependencies

    def overlap_with(self, other) -> set:
        """Return overlapping ASs between self and other."""
        return self.as_dependencies.intersection(other.as_dependencies)

    def get_overlap_percentage_with(self, other) -> float:
        """Return percentage of overlapping ASs between self and other,
        using self as the reference (i.e., self is 100%)."""
        # TODO If there are no dependencies in this scope, is the
        #  overlap always 100%?
        if not self.as_dependencies:
            return 100
        intersect = self.as_dependencies.intersection(other.as_dependencies)
        return (100 / len(self.as_dependencies)) * len(intersect)



