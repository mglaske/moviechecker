#!/usr/bin/env python
import os
import sys
import time
import logging
import json
import datetime
from media import MediaFile


class JsonDB(object):

    def __init__(self, filename):
        self.log = logging.getLogger()
        self.filename = filename
        self.db = {}
        self.path_index = {}
        self.write_immediate = False
        self.open = False
        self.save_interval = 20  # Every 100 new entries, lets save the database.
        self.dirty = False   # Track changes.
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

    def add(self, struct, filename, md5sum=""):
        if not md5sum:
            mfile = MediaFile(filename)
            md5sum = mfile.md5file()
        if not md5sum:
            self.log.error("db: unable to add entry without a key!")
            return False
        self.log.debug("db: add entry=%s", struct)
        self.db[md5sum] = struct
        self.dirty = True
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
            self.dirty = True
        for e in remove_paths:
            self.path_index.pop(e)

        if self.write_immediate:
            self.save()
        return

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
            self.dirty = False
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

    def close(self, save=False):
        if save:
            self.save()
        if self.dirty:
            self.save()
        self.clear()
        self.open = False
        return
