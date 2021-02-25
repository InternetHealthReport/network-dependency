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
