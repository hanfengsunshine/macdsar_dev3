from datetime import datetime
import calendar


def convert_datetime_to_timestamp(_time: datetime):
    return int(calendar.timegm(_time.timetuple()) * 1000000)

def convert_timestamp_to_datetime(ts: int):
    return datetime.utcfromtimestamp(ts / 1000000)