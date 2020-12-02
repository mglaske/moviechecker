#!/usr/bin/env python

import os
import sys
import re
import optparse
import logging
# import enzyme
from jsondb import JsonDB
from tables import Printer as TP
from media import MediaFile

class TVDB(JsonDB):

    def remove(self, show=None, season=None, episode=None, md5sum=None):
        remove = []
        remove_paths = []
        if md5sum:
            if md5sum in self.db:
                d = self.db[md5sum]
                what = self.name(d['show'], d['season'], d['episode'])
                self.log.info("tvdb: removing md5=%s show=%s", md5sum, what)
                remove.append(md5sum)
                remove_paths.append(self.db[md5sum]['filename'])
        if show and season and episode:
            for md5sum, m in self.db.iteritems():
                if m['show'].lower() == show.lower() and m['season'] == season and m['episode'] == episode:
                    what = self.name(show, season, episode)
                    self.log.info("tvdb: removing md5=%s show=%s", md5sum, what)
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

    def name(self, show, season, episode):
        return "%s.s%02de%02d" % (show, season, episode)

    def compare_names(self, oname, otest):
        name = oname.lower()
        test = otest.lower()
        name = re.sub(r'[^A-Za-z0-9]+', '', name)
        test = re.sub(r'[^A-Za-z0-9]+', '', test)
        self.log.debug("compare: name=(%s)=%s to test=(%s)=%s", oname, name, otest, test)
        return name == test

    def search(self, string, season=None, episode=None, show=None):
        results = []
        self.log.debug("Search for: string=%s season=%s episode=%s show=%s",
                       string, season, episode, show)
        for md5sum, details in self.db.iteritems():
            # Check this entry..
            if season and not int(season) == int(details['season']):
                continue
            if episode and not int(episode) == int(details['episode']):
                continue
            if show and not self.compare_names(details['show'], show):
                continue

            if string:
                if string.lower() in details['show'].lower():
                    results.append(details)
                elif string.lower() in details['title'].lower():
                    results.append(details)
            else:
                results.append(details)

        final_results = []
        for r in results:
            if not os.path.isfile(r['filename']):
                # Deleted tv file
                self.remove(md5sum=r['md5sum'])
                continue
            mfile = MediaFile(r["filename"])
            if not mfile.md5 == r["md5sum"]:
                self.log.warning("%s has a bad checksum!", r['filename'])
            final_results.append(r)
        return final_results

    def scan(self, startdir,
             extensions=['mkv', 'avi', 'mp4', 'mpeg', 'mpg', 'ts', 'flv', 'iso', 'm4v', 'divx', 'wmv'],
             ext_skip=['md5', 'idx', 'sub', 'srt', 'smi', 'nfo', 'nfo-orig', 'sfv', 'txt', 'json', 'jpeg', 'jpg', 'bak'],
             check=False, limit=0):
        """ Scan startdir for files that end in extensions,
            if check is set, check the md5 file against the actual md5
            checksum, and report
        """
        abspath = os.path.abspath(startdir)
        show_match = re.compile(r"^([^.]+)\.[Ss]{1}(\d+)[Ee]{1}(\d+)\.([^.]*)\.(\S+)\.[A-Za-z0-9]+$")
        found = 0
        for video_subdir, dirs, files in os.walk(abspath):
            if limit > 0 and found >= limit:
                break

            for filename in files:
                if limit > 0 and found >= limit:
                    self.log.info("SCAN LIMIT=%d SET, Stopping..", limit)
                    break

                fullpath = "%s/%s" % (video_subdir, filename)
                extension = filename.split('.')[-1].lower()
                filesize = os.path.getsize(fullpath)

                if extension in ext_skip:
                    continue

                if extension not in extensions:
                    self.log.warning("filename=%s is not in extensions list=%s, skipping",
                                     fullpath, extensions)
                    continue

                result = show_match.search(filename)
                if result:
                    (show, season, episode, title, remainder) = result.groups()
                    self.log.debug("filename=%s parsed into show=%s season=%s episode=%s title=%s remainder=(%s)",
                                   filename, show, season, episode, title, remainder)
                else:
                    self.log.debug("filename=%s unable to parse regex!", filename)
                    continue

                mfile = MediaFile(fullpath) 
                if mfile.md5:
                    self.log.debug('Found Hashfile: filename=%s MD5Hash=%s',
                                   filename, mfile.md5)
                    if check:
                        if mfile.check_checksum():
                            self.log.info('GOOD: %s [ %s / %s ]', filename,
                                          mfile.md5file, mfile.md5computed)
                        else:
                            self.log.error("BAD: Hash mismatch for file=%s "
                                           "stored_hash=%s computed_hash=%s!",
                                           filename, mfile.md5file, mfile.md5computed)
                else:
                    mfile.generate_checksum()

                if mfile.md5 in self.db:
                    self.db[mfile.md5]['valid'] = True
                    continue

                tv = {"show": show, "title": title,
                      "season": int(season), "episode": int(episode),
                      "filename": fullpath, "filetype": extension,
                      "filesize": self.bytes_to_human(filesize),
                      "mkvinfo": mfile.mediainfo(),
                      "md5sum": md5value, "valid": True}
                found += 1
                self.add(tv, fullpath, md5value)

                if found % self.save_interval == 0:
                    self.log.debug("Intermediate DB Save, found=%d interval=%d",
                                   found, self.save_interval)
                    self.save()

        # remove files that have been deleted
        self.clean_invalid()
        return


def printtvs(results=[], showkey=False, showpath=False):
    columns = ["Show", "Title", "S/E", "Duration", "Ext", "Resolution",
               "Bitrate", "Bits", "AudioC", "Formats", "Size"]
    t = TP()
    if showkey:
        columns.insert(0, "md5sum")
    if showpath:
        columns.append("Path")
    t.set_header(columns)
    t.justification["Duration"] = ">"
    t.justification["Bitrate"] = ">"
    t.justification["Resolution"] = ">"
    t.justification["Formats"] = ">"
    t.justification["Size"] = ">"
    t.justification["Bits"] = "^"

    for m in results:
        # format column data
        title = m["title"].replace(".", " ").replace("_", " ")
        s_e = "%s / %s" % (m['season'], m['episode'])
        key_s_e = "S%02dE%02d" % (m['season'], m['episode'])
        sortkey = "%s.%s;%s" % (m['show'], key_s_e, m['md5sum'])
        duration = "--"
        extension = m.get('filetype', '--').upper()
        filesize = m.get("filesize", -1)
        resolution, resname, bitrate = "--", "--", "--"
        channels, aformat, size = "--", "--", "--"
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

        # Build rows
        row = []
        if showkey:
            row.append(m["md5sum"])
        row.append(m["show"].replace("_", " "))
        row.append(title)
        row.append(s_e)
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

    sys.stdout.write(t.dump(header_underline=True, padding="  |  "))
    return


def main(options):
    db = TVDB(filename=options.dbfile)
    db.log = options.log
    options.log.info("Loaded %d tvs from database=%s",
                     len(db.db), options.dbfile)

    if options.delete:
        db.remove(md5sum=options.delete)

    if options.scan:
        db.scan(options.startdir, check=options.checkvideos, limit=options.limit)

    if options.search or options.show:
        results = db.search(options.search.lower(), options.season, options.episode, options.show)
        if len(results) > 0:
            printtvs(results, options.showkey, options.showpath)

    db.close()
    exit(0)


if __name__ == '__main__':
    usage = "Usage: %prog [options] arg"
    parser = optparse.OptionParser(usage, version="%prog 1.0")
    parser.add_option("-s", "--search", dest='search', type='string', help='Search string [%default]', default="")
    parser.add_option("-S", "--season", dest="season", type="string", help="Show just this season [%default]", default=None)
    parser.add_option("-E", "--episode", dest="episode", type="string", help="Show just this epiosode [%default]", default=None)
    parser.add_option("--show", dest="show", type="string", help="Search for this show exactly [%default]", default=None)
    parser.add_option("-d", "--delete", dest="delete", type="string", help="Delete hash key from database [%default]", default=None)
    parser.add_option("--db", dest="dbfile", type="string", help="Database file [%default]", default="/d1/tvshows/db.json")
    parser.add_option("--limit", dest="limit", type="int", help="Limit scan to only X entries", default=0)
    parser.add_option("--start-dir", dest="startdir", type="string", help="Start Directory to start processing tvs [%default]", default="/d1/tvshows/")
    parser.add_option("-l", "--log-level", dest="log_level", type='string', help="change log level [%default]", default='info')
    parser.add_option("-c", "--check-videos", dest="checkvideos", action="store_true", help="Check video MD5's to find bad ones [%default]", default=False)
    parser.add_option("--scan", dest="scan", action="store_true", help="Scan files in addition to search db [%default]", default=False)
    parser.add_option("--key", dest="showkey", action="store_true", help="Show Key value [%default]", default=False)
    parser.add_option("--path", dest="showpath", action="store_true", help="Show Filename Path [%default]", default=False)
    (options, args) = parser.parse_args()

    logger = logging.getLogger('lookup')
    level = options.log_level.upper()
    logger.setLevel(getattr(logging, level))
    stderr_handler = logging.StreamHandler()
    formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)
    options.log = logger

    main(options)
