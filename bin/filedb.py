#!/usr/bin/env python
import os
import sys
import fnmatch
import time
import re
import logging
import hashlib
import json
import sqlite3
import datetime


class FileDB(object):

    def __init__(self, filename):
        self.log = logging.getLogger()
        self.filename = filename
        self.connection = None
        self.cursor = None
        self.open = False
        self.write_immediate = False
        self.save_interval = 20  # Every 100 new entries, lets save the database.
        self.create_syntax = """CREATE TABLE IF NOT EXISTS files (hash TEXT PRIMARY KEY, filepath TEXT, details BLOB);
                                CREATE UNIQUE INDEX IF NOT EXISTS files.path ON files (filepath);"""
        self.load(filename)

    def _datetimehandler(self, o):
        if isinstance(o, datetime.datetime):
            return o.__str__()

    def create(self):
        if not self.open:
            return False
        with self.connection.cursor() as c:
            c.execute(self.create_syntax)
        return True

    def load(self, filename=None):
        filename = filename or self.filename
        try:
            self.connection = sqlite3.connect(filename)
            self.cursor = self.connection.cursor()
            self.open = True
        except IOError:
            self.log.error("db=%s is missing", filename)
        except Exception as e:
            self.log.error("unable to read db=%s: %s", filename, e)
        return self.open
   
    def add(self, struct, filename, md5sum=""):
        if not md5sum:
            md5sum = self.md5File(filename)
        self.log.debug("db: add entry=%s", struct)
        sql = """INSERT INTO files (hash, filename, details) VALUES (?, ?, ?);"""
        try:
            self.cursor.execute(sql, (md5sum, filename, struct))
            self.log.info("db: added filename=%s md5sum=%s to db",
                          os.path.basename(filename), md5sum)
            return True
        except Exception as e:
            self.log.error("db: failed insert filename=%s: %s", filename, e)

        if self.write_immediate:
            self.connection.commit()
        return False

    def remove(self, md5sum):
        details = self.get_hash(md5sum)
        if details:
            self.log.info("db: removing hash=%s filename=%s")
            sql = """DELETE FROM files WHERE hash=?"""
            try:
                self.cursor.execute(sql, md5sum.lower())
                return True
            except Exception as e:
                self.log.error("db: failed to delete md5sum=%s: %s", md5sum, e)
        else:
            self.log.error("db: remove hash=%s failed, no such hash!", md5sum)
        return False
                
        if self.write_immediate:
            self.connection.commit()
        return

    def check_hash(self, path):
        md5value = self.md5file(path)
        details = self.get_path(path)
        if details:
            if details[0][0] == md5value:
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
        sql = """SELECT * FROM files WHERE filename=?;"""
        try:
            self.cursor.execute(sql, (path,))
            return self.cursor.fetchone()
        except Exception as e:
            self.log.error("Unable to fetch hash=%s: %s", md5sum, e)
        return None

    def get_hash(self, md5sum):
        sql = """SELECT * FROM files WHERE hash=?;"""
        try:
            self.cursor.execute(sql, (md5sum,))
            return self.cursor.fetchone()
        except Exception as e:
            self.log.error("Unable to fetch hash=%s: %s", md5sum, e)
        return None
 
    def speed_to_human(self, bps, precision=2):
        mbps = bps/1000000.0
        return "%.*fMb/s" %(precision, mbps)

    def bytes_to_human(self, size, precision=2):
        suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
        suffixIndex = 0
        while size > 1024 and suffixIndex < 4:
            suffixIndex += 1    # increment the index of the suffix
            size = size/1024.0  # apply the division
        return "%.*f%s" % (precision, size, suffixes[suffixIndex])

    def ms_to_human(self, ms):
        seconds = float(ms) / 1000
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        return "%d:%02d:%02d" % (hours, minutes, seconds)

    def close(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()
        self.open = False
        return
