#!/usr/bin/env python
import datetime
import random
import string
import json
import socket
import struct
import time
import sys

def _timezone(utc_offset):
    '''
    Return a string representing the timezone offset.

    >>> _timezone(0)
    '+00:00'
    >>> _timezone(3600)
    '+01:00'
    >>> _timezone(-28800)
    '-08:00'
    >>> _timezone(-8 * 60 * 60)
    '-08:00'
    >>> _timezone(-30 * 60)
    '-00:30'
    '''
    # Python's division uses floor(), not round() like in other languages:
    #   -1 / 2 == -1 and not -1 / 2 == 0
    # That's why we use abs(utc_offset).
    hours = abs(utc_offset) // 3600
    minutes = abs(utc_offset) % 3600 // 60
    sign = (utc_offset < 0 and '-') or '+'
    return '%c%02d:%02d' % (sign, hours, minutes)

def _timedelta_to_seconds(td):
    '''
    >>> _timedelta_to_seconds(datetime.timedelta(hours=3))
    10800
    >>> _timedelta_to_seconds(datetime.timedelta(hours=3, minutes=15))
    11700
    >>> _timedelta_to_seconds(datetime.timedelta(hours=-8))
    -28800
    '''
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6

def _utc_offset(date, use_system_timezone):
    '''
    Return the UTC offset of `date`. If `date` does not have any `tzinfo`, use
    the timezone informations stored locally on the system.

    >>> if time.localtime().tm_isdst:
    ...     system_timezone = -time.altzone
    ... else:
    ...     system_timezone = -time.timezone
    >>> _utc_offset(datetime.datetime.now(), True) == system_timezone
    True
    >>> _utc_offset(datetime.datetime.now(), False)
    0
    '''
    if isinstance(date, datetime.datetime) and date.tzinfo is not None:
        return _timedelta_to_seconds(date.utcoffset())
    elif use_system_timezone:
        if date.year < 1970:
            # We use 1972 because 1970 doesn't have a leap day (feb 29)
            t = time.mktime(date.replace(year=1972).timetuple())
        else:
            t = time.mktime(date.timetuple())
        if time.localtime(t).tm_isdst: # pragma: no cover
            return -time.altzone
        else:
            return -time.timezone
    else:
        return 0

def _string(d, timezone):
    return ('%04d-%02d-%02dT%02d:%02d:%02d%s' %
            (d.year, d.month, d.day, d.hour, d.minute, d.second, timezone))

def format(date, utc=False, use_system_timezone=True):
    '''
    Return a string formatted according to the :RFC:`3339`. If called with
    `utc=True`, it normalizes `date` to the UTC date. If `date` does not have
    any timezone information, uses the local timezone::

        >>> d = datetime.datetime(2008, 4, 2, 20)
        >>> rfc3339(d, utc=True, use_system_timezone=False)
        '2008-04-02T20:00:00Z'
        >>> rfc3339(d) # doctest: +ELLIPSIS
        '2008-04-02T20:00:00...'

    If called with `use_system_timezone=False` don't use the local timezone if
    `date` does not have timezone informations and consider the offset to UTC
    to be zero::

        >>> rfc3339(d, use_system_timezone=False)
        '2008-04-02T20:00:00+00:00'

    `date` must be a `datetime.datetime`, `datetime.date` or a timestamp as
    returned by `time.time()`::

        >>> rfc3339(0, utc=True, use_system_timezone=False)
        '1970-01-01T00:00:00Z'
        >>> rfc3339(datetime.date(2008, 9, 6), utc=True,
        ...         use_system_timezone=False)
        '2008-09-06T00:00:00Z'
        >>> rfc3339(datetime.date(2008, 9, 6),
        ...         use_system_timezone=False)
        '2008-09-06T00:00:00+00:00'
        >>> rfc3339('foo bar')
        Traceback (most recent call last):
        ...
        TypeError: Expected timestamp or date object. Got <type 'str'>.

    For dates before January 1st 1970, the timezones will be the ones used in
    1970. It might not be accurate, but on most sytem there is no timezone
    information before 1970.
    '''
    # Try to convert timestamp to datetime
    try:
        if use_system_timezone:
            date = datetime.datetime.fromtimestamp(date)
        else:
            date = datetime.datetime.utcfromtimestamp(date)
    except TypeError:
        pass

    if not isinstance(date, datetime.date):
        raise TypeError('Expected timestamp or date object. Got %r.' %
                        type(date))

    if not isinstance(date, datetime.datetime):
        date = datetime.datetime(*date.timetuple()[:3])
    utc_offset = _utc_offset(date, use_system_timezone)
    if utc:
        # local time -> utc
        return _string(date - datetime.timedelta(seconds=utc_offset), 'Z')
    else:
        return _string(date, _timezone(utc_offset))

def string2numeric_hash(text):
    import hashlib
    return int(hashlib.md5(text).hexdigest()[:8], 16)

entry = {}
entry["dns"] = {}
entry["timestamp_start"] = format(datetime.datetime.now())
entry["timestamp_end"] = format(datetime.datetime.now() + + datetime.timedelta(minutes=1))
for i in range(random.randint(1, 20000)):
    rrname = ''.join(random.choice(string.ascii_lowercase) for _ in range(5))
    rdata = socket.inet_ntoa(struct.pack('>I', string2numeric_hash(rrname)))
    entry["dns"][(rrname + ".com")] = {"rdata": [{"rrtype": random.choice(["A", "NS", "MX"]), "rdata": rdata, "answering_host": "8.8.8.8", "count": 1, "rcode": "NOERROR"}]}

print(json.dumps(entry))
