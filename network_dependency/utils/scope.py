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
        intersect = self.overlap_with(other)
        return (100 / len(self.as_dependencies)) * len(intersect)

    def get_score_deltas(self, other) -> list:
        """Return list of tupels (as, score_diff) indicating the score
        difference for ASs that are contained in both self and other.

        Calculate the score difference as self.score - other.score."""
        return [(as_, self.hegemony_scores[as_] - other.hegemony_scores[as_])
                for as_ in self.overlap_with(other)]

    def get_missing_score_sum(self, other) -> float:
        """Return the sum of scores for ASs that are contained in self
        but not in other."""
        return sum([self.hegemony_scores[as_] for as_ in self.not_in(other)])

    def __get_rank_lists(self, other) -> (list, list):
        intersect = self.overlap_with(other)
        self_score_list = list()
        other_score_list = list()
        for as_ in intersect:
            self_score_list.append((self.hegemony_scores[as_], as_))
            other_score_list.append((other.hegemony_scores[as_], as_))
        self_score_list.sort(reverse=True)
        other_score_list.sort(reverse=True)
        return self_score_list, other_score_list

    def get_rank_difference_number(self, other) -> (int, float):
        """Calculate the number and percentage of rank differences and
        return them as a tuple (number, percentage).

        Compute the AS intersection between self and other and compare
        the remaining ASs in descending score order. The intersection is
        the reference for the percentage and counts as 100%."""
        self_rank_list, other_rank_list = self.__get_rank_lists(other)
        # TODO: No overlap means no difference?
        if len(self_rank_list) == 0:
            return 0, 0
        difference_count = 0
        for idx, (_, as_) in enumerate(self_rank_list):
            if as_ != other_rank_list[idx][1]:
                difference_count += 1
        return difference_count, \
               (100 / len(self_rank_list)) * difference_count

    def get_rank_difference_magnitudes(self, other) -> list:
        """Calculate the magnitude of rank differences and return them
        in a list.

        The magnitude refers to the positional difference between the
        ranks of one AS in self and other. It is calculated with
        self.pos - other.pos."""
        self_rank_list, other_rank_list = self.__get_rank_lists(other)
        ret = list()
        for idx, (_, as_) in enumerate(self_rank_list):
            # Retrieve the index of as_ in other_rank_list.
            # See https://stackoverflow.com/a/10865345
            other_idx = next(i for i, v in enumerate(other_rank_list)
                             if v[1] == as_)
            if idx == other_idx:
                continue
            ret.append((as_, idx, idx - other_idx))
        return ret
