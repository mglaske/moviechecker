#!/usr/bin/env python

import os
import sys
import optparse
import logging
from jsondb import JsonDB
from media import MediaFile
from tables import Printer as TP


class MovieDB(JsonDB):

    def remove(self, title=None, year=None, md5sum=None):
        remove = []
        remove_paths = []
        if md5sum:
            if md5sum in self.db:
                self.log.warning("moviedb: removing md5=%s title=%s year=%s",
                                 md5sum, self.db[md5sum]['title'],
                                 self.db[md5sum]['year'])
                remove.append(md5sum)
                remove_paths.append(self.db[md5sum]['filename'])
        if title and year:
            for md5sum, m in self.db.iteritems():
                if m['title'].lower() == title.lower() and m['year'] == year:
                    self.log.info("moviedb: removing md5=%s title=%s year=%s",
                                  md5sum, title, year)
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

    def search(self, string="", resolution=None, year=None):
        results = []
        for md5sum, details in self.db.iteritems():
            append = True
            if string != "" and string.lower() not in details['title'].lower():
                append = False
            if resolution:
                mkvinfo = details.get("mkvinfo", {})
                videos = mkvinfo.get("video")
                if len(videos) > 0:
                    res = videos[0].get("resname", "")
                    if res != resolution:
                        append = False
            if year and details.get("year", 0) != year:
                append = False
            if append:
                results.append(details)
        final_results = []
        for r in results:
            if not os.path.isfile(r['filename']):
                # Deleted movie file
                self.remove(md5sum=r['md5sum'])
                continue
            mfile = MediaFile(r["filename"])
            if mfile.md5 != r["md5sum"]:
                self.log.warning("%s has a bad checksum!", r['filename'])
            final_results.append(r)
        return final_results

    def scan(self, startdir,
             extensions=['mkv', 'avi', 'mp4', 'mpeg', 'mpg', 'ts', 'flv', 'iso', 'm4v'],
             ext_skip=['md5', 'idx', 'sub', 'srt', 'smi', 'nfo', 'sfv', 'txt', 'json', 'jpeg', 'jpg', 'bak'],
             check=False):
        """ Scan startdir for files that end in extensions,
            if check is set, check the md5 file against the actual md5
            checksum, and report
        """
        abspath = os.path.abspath(startdir)
        for basepath, dirs, files in os.walk(abspath):
            for filename in files:
                fullpath = "%s/%s" % (basepath, filename)
                extension = filename.split('.')[-1].lower()
                basename = '.'.join(filename.split('.')[0:-1])
                filesize = os.path.getsize(fullpath)

                if extension in ext_skip:
                    continue

                if extension not in extensions:
                    self.log.warning("filename=%s is not in extensions list=%s, skipping",
                                     fullpath, extensions)
                    continue

                video_subdir = basepath
                video_year = 'n/a'
                video_name = filename
                video_genre = 'n/a'
                try:
                    video_subdir = basepath.split('/')[-1]
                    video_year = video_subdir.split('.')[-1]
                    video_name = ".".join(video_subdir.split('.')[0:-1])
                    video_genre = basepath.split('/')[-2]
                except Exception as e:
                    self.log.error("Error parsing video path=%s: %s", fullpath, e)
                self.log.debug('Found (%s): BasePath=%s Filenam=%s Basename=%s Extension=%s',
                               fullpath, basepath, filename, basename, extension)

                mfile = MediaFile(fullpath)
                md5value = mfile.md5
                if md5value:
                    self.log.debug('Found Hashfile: filename=%s MD5Hash=%s',
                                   filename, md5value)
                    if check:
                        if mfile.check_checksum():
                            self.log.info('GOOD: %s [ %s / %s ]', filename,
                                          md5value, mfile.md5computed)
                        else:
                            self.log.error("BAD: Hash mismatch for file=%s "
                                           "stored_hash=%s computed_hash=%s!",
                                           filename, md5value, mfile.md5computed)
                else:
                    md5value = mfile.generate_checksum()

                if md5value in self.db:
                    self.db[md5value]['valid'] = True
                    continue

                movie = {"title": video_name, "year": video_year,
                         "genre": video_genre,
                         "filename": fullpath, "filetype": extension,
                         "filesize": self.bytes_to_human(filesize),
                         "mkvinfo": mfile.mediainfo(),
                         "md5sum": md5value,
                         "valid": True}

                self.add(movie, fullpath, md5value)

        # remove files that have been deleted
        self.clean_invalid()
        return


def printmovies(results=[], showkey=False, showpath=False):
    columns = ["Genre", "Title", "Year", "Duration", "EXT", "Resolution",
               "Bitrate", "Bits", "AudioC", "Formats", "Size"]
    if showkey:
        columns.insert(0, "md5sum")
    if showpath:
        columns.append("Path")
    t = TP()
    t.set_header(columns, justification="<")
    t.justification["Duration"] = ">"
    t.justification["Bitrate"] = ">"
    t.justification["Resolution"] = ">"
    t.justification["Formats"] = ">"
    t.justification["Size"] = ">"
    t.justification["Bits"] = "^"

    for m in results:
        title = m["title"].replace(".", " ")
        extension = m.get("filetype", "--").upper()
        filesize = m.get("filesize", -1)
        resolution, resname, bitrate = "--", "--", "--"
        channels, aformat, duration = "--", "--", "--"
        if m['mkvinfo']:
            mkvinfo = m['mkvinfo']
            video = mkvinfo.get('video', [])
            audio = mkvinfo.get('audio', [])
            if len(video) > 0:
                resolution = video[0].get('resolution', '--')
                resname = video[0].get('resname', '--')
                bitrate = video[0].get('bit_rate', '--')
                bitdepth = str(video[0].get('bit_depth', '--'))
            audio_tracks = len(audio)
            if audio_tracks > 0:
                channels = audio[0].get('channels', '--') or "--"
                channels = str(channels).replace("Object Based / ", "")
            formats = [x.get('format') for x in audio if x.get('format')]
            aformat = "/".join(formats)
            duration = str(mkvinfo.get('duration', '--')).split('.')[0]

        row = []
        sortkey = "%s;%s;%s" % (m['title'], resname, m['md5sum'])
        if showkey:
            row.append(m["md5sum"])
        row.append(m["genre"])
        row.append(title)
        row.append(m["year"])
        row.append(duration)
        row.append(extension)
        row.append("%s (%s)" % (resolution, resname))
        row.append(bitrate)
        row.append(bitdepth)
        row.append(channels)
        row.append(aformat)
        row.append(filesize)
        if showpath:
            row.append(m["filename"])
        t.add_data(row, key=sortkey)

    sys.stdout.write("%s\n" % t.dump(header_underline=True, padding="  |  "))
    return


def main(options):
    db = MovieDB(filename=options.dbfile)
    db.log = options.log
    logging.info("Loaded %d movies from database=%s",
                 len(db.db), options.dbfile)

    if options.delete:
        db.remove(md5sum=options.delete)

    if options.scan:
        db.scan(options.startdir)

    if options.search or options.s_res:
        results = db.search(options.search.lower(), resolution=options.s_res, year=options.s_year)
        if len(results) > 0:
            printmovies(results, options.showkey, options.showpath)

    db.close()
    exit(0)


if __name__ == '__main__':
    usage = "Usage: %prog [options] arg"
    parser = optparse.OptionParser(usage, version="%prog 1.0")
    parser.add_option("-s", "--search", dest="search", type="string", help="Search string [%default]", default="")
    parser.add_option("--resolution", dest="s_res", type="string", help="Search for files with [%default] resolution", default=None)
    parser.add_option("--year", dest="s_year", type="string", help="Search for files with [%default] year", default=None)
    parser.add_option("-d", "--delete", dest="delete", type="string", help="Delete hash key from database [%default]", default=None)
    parser.add_option("--db", dest="dbfile", type="string", help="Database file [%default]", default="/d1/movies/db.json")
    parser.add_option("--start-dir", dest="startdir", type="string", help="Start Directory to start processing movies [%default]", default="/d1/movies/")
    parser.add_option("-l", "--log-level", dest="log_level", type="string", help="change log level [%default]", default="info")
    parser.add_option("-c", "--check-videos", dest="checkvideos", action="store_true", help="Check video MD5s to find bad ones [%default]", default=False)
    parser.add_option("--scan", dest="scan", action="store_true", help="Scan files in addition to search db [%default]", default=False)
    parser.add_option("--key", dest="showkey", action="store_true", help="Show Key value [%default]", default=False)
    parser.add_option("--path", dest="showpath", action="store_true", help="Show file path [%default]", default=False)
    (options, args) = parser.parse_args()

    logger = logging.getLogger('')
    level = options.log_level.upper()
    logger.setLevel(getattr(logging, level))
    stderr_handler = logging.StreamHandler()
    formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)
    options.log = logger

    main(options)
