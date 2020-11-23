#!/usr/bin/env python
import os
import sys
import time
import logging
import hashlib
import json
import datetime


class JsonDB(object):

    def __init__(self, filename):
        self.log = logging.getLogger()
        self.filename = filename
        self.db = {}
        self.path_index = {}
        self.write_immediate = False
        self.open = False
        self.save_interval = 20  # Every 100 new entries, lets save the database.
        self.load(filename)

    def _datetimehandler(self, o):
        if isinstance(o, datetime.datetime):
            return o.__str__()

    def clear(self):
        self.db = {}
        self.path_index = {}
        return

    def load(self, filename=None):
        filename = filename or self.filename
        if os.path.isfile(filename):
            try:
                with open(filename, 'r') as fh:
                    self.db = json.load(fh)
                self.open = True
                self.index()
            except Exception as e:
                self.log.error("unable to read db=%s: %s", filename, e)
        return self.open

    def index(self):
        # Build path index
        if not self.open:
            return False
        for md5, details in self.db.iteritems():
            self.path_index[details['filename']] = md5
        return True

    def add(self, struct, filename,
            md5sum=""):
        if md5sum == "":
            md5sum = self.md5File(filename)
        self.log.debug("db: add entry=%s", struct)
        self.db[md5sum] = struct
        self.path_index[filename] = md5sum
        self.log.info("db: adding filename=%s md5sum=%s to db",
                      os.path.basename(filename), md5sum)
        if self.write_immediate:
            self.save()
        return True

    def remove(self, md5sum=None):
        remove = []
        remove_paths = []
        if md5sum:
            if md5sum in self.db:
                d = self.db[md5sum]
                what = self.name(d['show'], d['season'], d['episode'])
                self.log.info("db: removing md5=%s show=%s", md5sum, what)
                remove.append(md5sum)
                remove_paths.append(self.db[md5sum]['filename'])
        for e in remove:
            self.db.pop(e)
        for e in remove_paths:
            self.path_index.pop(e)

        if self.write_immediate:
            self.save()
        return

    def check_hash(self, path):
        md5value = self.md5file(path)
        if self.path_index[path] == md5value:
            return True
        return False

    def md5filename(self, path):
        splits = path.split('.')
        base = ".".join(splits[0:-1])
        md5file = base + ".md5"
        return md5file

    def md5file(self, path):
        # given a path, pull the md5 from the file
        md5file = self.md5filename(path)
        if os.path.isfile(md5file):
            try:
                with open(md5file, 'r') as fh:
                    md5value = fh.readline().split()[0].lower()
                return md5value
            except Exception as e:
                self.log.error("Unable to get md5file=%s: %s", md5file, e)
        else:
            # Generate missing md5 file.
            return self.generate_checksum(path)
        return None

    def md5Checksum(self, path):
        try:
            m = hashlib.md5()
            with open(path, 'rb') as fh:
                while True:
                    data = fh.read(8192)
                    if not data:
                        break
                    m.update(data)
            return m.hexdigest()
        except Exception as e:
            self.log.error("Unable to compute checksum of (%s): %s", path, e)
        return None

    def generate_checksum(self, path):
        filename = os.path.basename(path)
        self.log.info('Generating hash for (%s)', filename)
        md5value = self.md5Checksum(path)
        if not md5value:
            return None
        md5file = self.md5filename(path)
        try:
            with open(md5file, 'w') as fh:
                fh.write(md5value + "\t" + filename)
            self.log.info('Wrote computed value (%s) for filename (%s)',
                          md5value, os.path.basename(md5file))
        except Exception as e:
            self.log.error("Unable to write checksum file (%s): %s",
                           md5file, e)
        return md5value

    def get_path(self, path):
        if path in self.path_index:
            md5 = self.path_index[path]
            return self.db[md5]
        return None

    def save(self, filename=None):
        if len(self.db) < 1:
            self.log.warning("db: save called on empty database, skipping")
            return
        if not self.open:
            return
        filename = filename or self.filename
        lockfile = filename + ".lock"
        if os.path.isfile(lockfile):
            if (time.time() - os.path.getmtime(lockfile)) > 200:
                self.log.warning("db: lockfile=%s is stale, removing!", lockfile)
                os.unlink(lockfile)
            else:
                sys.stdout.write("db: locked, waiting..")
                while True:
                    sys.stdout.write(".")
                    for t in range(1, 100):
                        time.sleep(.1)
                    if not os.path.isfile(lockfile):
                        break

        self.log.info("db: saving filename=%s with (%d) entries",
                      filename, len(self.db))
        tmpfile = "%s.tmp" % filename
        try:
            open(lockfile, 'w').write("locked")
            with open(tmpfile, 'w') as fh:
                json.dump(self.db, fh, default=self._datetimehandler)
            os.rename(tmpfile, filename)
        except Exception as e:
            self.log.error("unable to write db=%s: %s", filename, e)
            if os.path.isfile(tmpfile):
                os.unlink(tmpfile)
        os.unlink(lockfile)
        return

    def clean_invalid(self):
        for e, d in self.db.iteritems():
            if not d.get('valid', True):
                self.remove(md5sum=e)
        return

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

    def close(self, save=False):
        if save:
            self.save()
        self.clear()
        self.open = False
        return
