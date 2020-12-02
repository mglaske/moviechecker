#!/usr/bin/env python
import os
import logging
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

    def get_path(self, path):
        sql = """SELECT * FROM files WHERE filename=?;"""
        try:
            self.cursor.execute(sql, (path,))
            return self.cursor.fetchone()
        except Exception as e:
            self.log.error("Unable to fetch path=%s: %s", path, e)
        return None

    def get_hash(self, md5sum):
        sql = """SELECT * FROM files WHERE hash=?;"""
        try:
            self.cursor.execute(sql, (md5sum,))
            return self.cursor.fetchone()
        except Exception as e:
            self.log.error("Unable to fetch hash=%s: %s", md5sum, e)
        return None

    def close(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()
        self.open = False
        return
