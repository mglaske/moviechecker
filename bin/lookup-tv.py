#!/usr/bin/env python

import os
import sys
import re
import optparse
import logging
# import enzyme
from jsondb import JsonDB
from tables import Printer as TP
from pymediainfo import MediaInfo


class TVDB(JsonDB):

    def name(self, show, season, episode):
        return "%s.s%02de%02d" % (show, season, episode)

    def add(self, show, season, episode, title, filename,
            filetype="n/a", filesize=0,
            mkvinfo=None, md5sum=""):
        tv = {
            "show": show,
            "season": season,
            "episode": episode,
            "title": title,
            "filename": filename,
            "filetype": filetype,
            "filesize": filesize,
            "mkvinfo": mkvinfo,
            "md5sum": md5sum,
            "valid": True
        }
        self.log.debug("tvdb: add entry=%s", tv)
        self.db[md5sum] = tv
        self.path_index[filename] = md5sum
        what = self.name(show, season, episode)
        self.log.info("tvdb: adding filename=%s show=%s md5sum=%s to db",
                      os.path.basename(filename), what, md5sum)
        if self.write_immediate:
            self.save()
        return

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
        for e in remove_paths:
            self.path_index.pop(e)

        if self.write_immediate:
            self.save()
        return

    def search(self, string, season=None, episode=None):
        results = []
        for md5sum, details in self.db.iteritems():
            # Check this entry..
            if season and not int(season) == int(details['season']):
                continue
            if episode and not int(episode) == int(details['episode']):
                continue

            if string.lower() in details['show'].lower():
                results.append(details)
            elif string.lower() in details['title'].lower():
                results.append(details)

        final_results = []
        for r in results:
            if not os.path.isfile(r['filename']):
                # Deleted tv file
                self.remove(md5sum=r['md5sum'])
                continue
            if not self.check_hash(r['filename']):
                self.log.warning("%s has a bad checksum!", r['filename'])
            final_results.append(r)
        return final_results

    def scan(self, startdir,
             extensions=['mkv', 'avi', 'mp4', 'mpeg', 'mpg', 'ts', 'flv', 'iso', 'm4v'],
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

                # self.log.debug('Found (%s): BasePath=%s Filename=%s Basename=%s Extension=%s',
                #               fullpath, video_subdir, filename, basename, extension)

                md5value = self.md5file(fullpath)
                if md5value:
                    self.log.debug('Found Hashfile: filename=%s MD5Hash=%s',
                                   filename, md5value)
                    if check:
                        checkvalue = self.md5Checksum(filename).lower()
                        if md5value != checkvalue:
                            self.log.error("BAD: Hash mismatch for file=%s "
                                           "stored_hash=%s computed_hash=%s!",
                                           filename, md5value, checkvalue)
                        else:
                            self.log.info('GOOD: %s [ %s / %s ]', filename,
                                          md5value, checkvalue)
                else:
                    md5value = self.generate_checksum(fullpath)

                if md5value in self.db:
                    self.db[md5value]['valid'] = True
                    continue

                mediainfo = None
                mediainfo = self.mediainfo(fullpath)

                found += 1
                self.add(show=show, season=int(season), episode=int(episode),
                         title=title,
                         filename=fullpath, md5sum=md5value,
                         filetype=extension,
                         filesize=self.bytes_to_human(filesize),
                         mkvinfo=mediainfo)

                if found % self.save_interval == 0:
                    self.log.debug("Intermediate DB Save, found=%d interval=%d",
                                   found, self.save_interval)
                    self.save()

        # remove files that have been deleted
        self.clean_invalid()
        return

    def mediainfo(self, path):
        info = {'title': None, 'duration': None, 'chapters': None, 'video': [], 'audio': []}
        video = {'height': None, 'width': None, 'resolution': None, 'resname': None, 'codec': None, 'duration': None, 'bit_rate': None, 'bit_depth': None, 'aspect_ratio': None}
        audio = {'freq': None, 'channels': None, 'language': None, 'bit_depth': None, 'codec': None}
        try:
            mi = MediaInfo.parse(path)
        except Exception as e:
            self.log.error("MediaInfo threw error reading path=%s: %s", path, e)
            return {}

        for t in mi.tracks:
            if t.track_type == 'General':
                info['duration'] = self.ms_to_human(t.duration or 0)
                continue
            if t.track_type == "Video":
                vt = dict(video)
                try:
                    # note, display_aspect_ratio = 1.791 , eg 16:9
                    vt['aspect_ratio'] = t.other_display_aspect_ratio[0]
                except IndexError:
                    vt['aspect_ratio'] = 'n/a'
                vt['height'] = t.height
                vt['width'] = t.width
                vt['resolution'] = "%dx%d" % (t.width, t.height)
                if 'lace' in (t.scan_type or ""):
                    scantype = 'i'
                else:
                    scantype = 'p'
                if 1601 > t.height > 1080:
                    resname = "2160"
                elif 1081 > t.height > 720:
                    resname = "1080"
                elif 721 > t.height > 480:
                    resname = "720"
                elif 481 > t.height > 320:
                    resname = "320"
                else:
                    resname = "%s" % t.height
                vt['resname'] = "%s%s" % (resname, scantype)
                vt['frame_rate'] = t.frame_rate
                vt['codec'] = t.codec
                vt['bit_depth'] = t.bit_depth
                if t.bit_rate:
                    br = t.bit_rate
                elif t.nominal_bit_rate:
                    br = t.nominal_bit_rate
                else:
                    br = None
                try:
                    vt['bit_rate'] = self.speed_to_human(br)
                except Exception:
                    vt['bit_rate'] = "n/a"

                info['video'].append(vt)
            if t.track_type == "Audio":
                at = dict(audio)
                at['codec'] = t.codec_family
                at['format'] = t.format
                at['bit_depth'] = t.bit_depth
                at['language'] = t.language
                at['channels'] = t.channel_s
                at['freq'] = t.sampling_rate
                info['audio'].append(at)
        return info


def printtvs(results=[], showkey=False, showpath=False):
    columns = ["Show", "Title", "S/E", "Duration", "Ext", "Resolution",
               "Bitrate", "AudioC", "Formats", "Size"]
    t = TP()
    if showkey:
        columns.insert(0, "md5sum")
    if showpath:
        columns.append("Path")
    t.set_header(columns)
    t.justification["Duration"] = ">"
    t.justification["Bitrate"] = ">"
    t.justification["Formats"] = ">"
    t.justification["Size"] = ">"

    for m in results:
        # format column data
        title = m["title"].replace(".", " ").replace("_", " ")
        s_e = "%s / %s" % (m['season'], m['episode'])
        key_s_e = "S%02dE%02d" % (m['season'], m['episode'])
        sortkey = "%s.%s;%s" % (m['show'], key_s_e, m['md5sum'])
        duration = "--"
        extension = m['filename'][-3:].upper()
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

    if options.search:
        results = db.search(options.search.lower(), options.season, options.episode)
        if len(results) > 0:
            printtvs(results, options.showkey, options.showpath)

    db.close()
    exit(0)


if __name__ == '__main__':
    usage = "Usage: %prog [options] arg"
    parser = optparse.OptionParser(usage, version="%prog 1.0")
    parser.add_option("-s", "--search", dest='search', type='string', help='Search string [%default]', default=None)
    parser.add_option("-S", "--season", dest="season", type="string", help="Show just this season [%default]", default=None)
    parser.add_option("-E", "--episode", dest="episode", type="string", help="Show just this epiosode [%default]", default=None)
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

    # enzyme_logger = logging.getLogger("enzyme")
    # enzyme_logger.setLevel(logging.ERROR)

    main(options)
