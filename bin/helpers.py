#!/usr/bin/env python

def speed_to_human(self, bps, precision=2):
    mbps = bps / 1000000.0
    return "%.*fMb/s" % (precision, mbps)

def bytes_to_human(self, size, precision=2):
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
    suffixIndex = 0
    while size > 1024 and suffixIndex < 4:
        suffixIndex += 1    # increment the index of the suffix
        size = size / 1024.0  # apply the division
    return "%.*f%s" % (precision, size, suffixes[suffixIndex])

def ms_to_human(self, ms):
    seconds = float(ms) / 1000
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    return "%d:%02d:%02d" % (hours, minutes, seconds)

