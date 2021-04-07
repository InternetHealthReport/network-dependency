from datetime import datetime, timezone


def convert_date_to_epoch(s) -> int:
    """Parse date from config file with format %Y-%m-%dT%H:%M and return
    it as a UNIX epoch in seconds.

    Return 0 if the date can not be parsed."""
    try:
        return int(datetime.strptime(s, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        # Not a valid date:
        return 0


def parse_timestamp_argument(arg: str) -> int:
    """Parse a timestamp argument which can either be given as a UNIX
    epoch in seconds or milliseconds, or as a date with format
    %Y-%m-%dT%H:%M and return it as a UNIX epoch in seconds.

    Return 0 if the timestamp can not be parsed."""
    if arg is None:
        return 0
    if arg.isdigit():
        if len(arg) == 10:
            # Already epoch in seconds.
            return int(arg)
        elif len(arg) == 13:
            # Epoch in milliseconds
            return int(arg) // 1000
        # Invalid format.
        return 0
    return convert_date_to_epoch(arg)


def parse_range_argument(arg: str) -> list:
    """Parse a range argument which can either be a single integer, a
    comma-separated list of integers, or in a range specification with
    format start:end:step.

    Return an empty list if the argument can not be parsed."""
    if arg is None:
        return list()
    if arg.isdigit():
        return [int(arg)]
    if ',' in arg:
        values = arg.split(',')
        ret = list()
        for v in values:
            if not v.isdigit():
                return list()
            ret.append(int(v))
        return ret
    if ':' in arg:
        arg_split = arg.split(':')
        if len(arg_split) != 3:
            return list()
        for v in arg_split:
            if not v.isdigit():
                return list()
        range_spec = tuple(map(int, arg_split))
        return [i for i in range(*range_spec)]
    return list()
