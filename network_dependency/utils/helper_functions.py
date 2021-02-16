from datetime import datetime, timezone


def convert_date_to_epoch(s) -> int:
    """Parse date from config file"""
    try:
        return int(datetime.strptime(s, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        # Not a valid date:
        return 0
