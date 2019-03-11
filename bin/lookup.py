#!/usr/bin/env python

import os,sys,fnmatch,time
import optparse,logging
import enzyme
import hashlib
import json
import datetime
#from hachoir_core.cmd_line import unicodeFilename
#from hachoir_core.i18n import getTerminalCharset
#from hachoir_metadata import extractMetadata
#from hachoir_parser import createParser

class MovieDB(object):

    def __init__(self, filename):
        self.log = logging.getLogger(__name__)
        self.filename = filename
        self.db = {}
        self.write_immediate = False
        self.open = False
        self.load(filename)

    def _datetimehandler(self, o):
        if isinstance(o, datetime.datetime):
            return o.__str__()

    def load(self, filename=None):
        filename = filename or self.filename
        if os.path.isfile(filename):
            try:
                with open(filename, 'r') as fh:
                    self.db = json.load(fh)
                self.open = True
            except Exception as e:
                self.log.error("unable to read db=%s: %s", filename, e)
        return self.open
   
    def add(self, title, year, genre, filename,
            filetype="n/a", filesize=0,
            mkvinfo=None, md5sum=""):
        movie = {
            "title": title,
            "year": year,
            "genre": genre,
            "filename": filename,
            "filetype": filetype,
            "filesize": filesize,
            "mkvinfo": mkvinfo,
            "md5sum": md5sum
        }
        self.db[md5sum] = movie
        if self.write_immediate:
            self.save()
        return

    def remove(self, title=None, year=None, md5sum=None):
        if md5sum:
            if md5sum in self.db:
                self.log.info("movideb: removing md5=%s title=%s year=%s",
                              md5sum, self.db[md5sum]['title'],
                              self.db[md5sum]['year'])
                self.db.pop(md5sum)
        if title and year:
            for md5sum, m in self.db.iteritems():
                if m['title'].lower() == title.lower() and m['year'] == year:
                    self.log.info("moviedb: removing md5=%s title=%s year=%s",
                                  md5sum, title, year)
                    self.db.pop(md5sum)
        if self.write_immediate:
            self.save()
        return

    def search(self, string):
        results = []
        for md5sum, details in self.db.iteritems():
            if string.lower() in details['title'].lower():
                results.append(details)
        return results
    
    def save(self, filename=None):
        filename = filename or self.filename
        try:
            with open(filename, 'w') as fh:
                json.dump(self.db, fh, default=self._datetimehandler)
            self.log.info("moviedb: saving filename=%s with (%d) entries",
                          filename, len(self.db))
        except Exception as e:
            self.log.error("unable to write db=%s: %s", filename, e)
        return

    def close(self):
        self.save()
        self.open = False
        return


def md5Checksum(filePath):
    with open(filePath, 'rb') as fh:
        m = hashlib.md5()
        while True:
            data = fh.read(8192)
            if not data:
                break
            m.update(data)
        return m.hexdigest()

def GetHumanReadable(size,precision=2):
    suffixes=['B','KB','MB','GB','TB']
    suffixIndex = 0
    while size > 1024 and suffixIndex < 4:
        suffixIndex += 1 #increment the index of the suffix
        size = size/1024.0 #apply the division
    return "%.*f%s"%(precision,size,suffixes[suffixIndex])


def _mkvinfonew(filename):
    info = { 'title': None, 'duration': None, 'chapters': None, 'video': [], 'audio': [] }
    video = { 'height': None, 'width': None, 'resolution': None, 'resname': None, 'codec': None, 'duration': None }
    audio = { 'freq': None, 'channels': None, 'language': None, 'bit_depth': None, 'codec': None }
    try:
        charset = getTerminalCharset()
        filename, real_filename = unicodeFilename(filename, charset), filename
        parser = createParser(filename, real_filename=real_filename)
        md = extractMetadata(parser)
    except Exception, e:
        logging.error("Could not open (%s) with enzyme for getting video info! %s",filename, e)
        return info

    try:
        info['title'] = mkv.info.title
        info['duration'] = mkv.info.duration
        info['chapters'] = [ c.start for c in mkv.chapters ]
        for v in mkv.video_tracks:
            vt = dict(video)
            vt['height'] = v.height
            vt['width'] = v.width
            vt['resolution'] = "%dx%d" % (v.width,v.height)
            if v.interlaced:
                scantype = 'i'
            else:
                scantype = 'p'
            if 1081 > v.height > 720:
                vt['resname'] = '1080%s' % scantype
            elif 721 > v.height > 480:
                vt['resname'] = '720%s' % scantype
            elif 481 > v.height > 320:
                vt['resname'] = '320%s' % scantype
            else:
                vt['resname'] = '%d%s' % (v.height,scantype)
             
            vt['codec'] = v.codec_id
            info['video'].append( vt )
        for a in mkv.audio_tracks:
            at = dict(audio)
            at['codec'] = a.codec_id
            at['bit_depth'] = a.bit_depth
            at['language'] = a.language
            at['channels'] = a.channels
            at['freq'] = a.sampling_frequency
            info['audio'].append( at )
    except Exception, e:
        logging.error("Unable to parse mkv! %s",e)
    return info


def _mkvinfo(filename):
    info = { 'title': None, 'duration': None, 'chapters': None, 'video': [], 'audio': [] }
    video = { 'height': None, 'width': None, 'resolution': None, 'resname': None, 'codec': None, 'duration': None }
    audio = { 'freq': None, 'channels': None, 'language': None, 'bit_depth': None, 'codec': None }
    try:
        with open(filename,'rb') as fh:
            mkv = enzyme.MKV(fh)
            logging.debug("enzyme decoded into: %s", mkv)
    except Exception, e:
        logging.error("Could not open (%s) with enzyme for getting video info! %s",filename, e)
        return info

    try:
        info['title'] = mkv.info.title
        info['duration'] = str(mkv.info.duration)
        info['chapters'] = [ c.start for c in mkv.chapters ]
        for v in mkv.video_tracks:
            vt = dict(video)
            vt['height'] = v.height
            vt['width'] = v.width
            vt['resolution'] = "%dx%d" % (v.width,v.height)
            if v.interlaced:
                scantype = 'i'
            else:
                scantype = 'p'
            if 1081 > v.height > 720:
                vt['resname'] = '1080%s' % scantype
            elif 721 > v.height > 480:
                vt['resname'] = '720%s' % scantype
            elif 481 > v.height > 320:
                vt['resname'] = '320%s' % scantype
            else:
                vt['resname'] = '%d%s' % (v.height,scantype)
             
            vt['codec'] = v.codec_id
            info['video'].append( vt )
        for a in mkv.audio_tracks:
            at = dict(audio)
            at['codec'] = a.codec_id
            at['bit_depth'] = a.bit_depth
            at['language'] = a.language
            at['channels'] = a.channels
            at['freq'] = a.sampling_frequency
            info['audio'].append( at )
    except Exception, e:
        logging.error("Unable to parse mkv! %s",e)
    return info
   

def printmovies(results=[]):
    # sort
    rs = {}
    for m in results:
        rs[m['title']] = m

    for key in sorted(rs.keys()):
        m = rs[key]
        sys.stdout.write( "{0:<12s}  {1:<50s}  {2:<5s}".format(m['genre'], m['title'], m['year']) )

        extension = m['filename'][-3:]
        if m['mkvinfo']:
            resolution = "n/a"
            resname = "n/a"
            channels = -1
            mkvinfo = m['mkvinfo']
            video = mkvinfo.get('video', [])
            audio = mkvinfo.get('audio', [])
            if len(video) > 0:
                resolution = video[0].get('resolution', 'n/a')
                resname = video[0].get('resname', 'n/a')
            if len(audio) > 0:
                channels = audio[0].get('channels', 'n/a')
            duration = str(mkvinfo.get('duration','n/a')).split('.')[0]

            res = "%s (%s)" % (resolution, resname)
            sys.stdout.write("  {0:<10s}  Type={1:<3s}  Resolution={2:<18s}  Audio_Channels={3:<2d}  Size={4:<15s}".format(duration, extension.upper(), res, channels, m.get('filesize', -1)))

        sys.stdout.write("\n")
    return


def main(options):
    db = MovieDB(filename=options.dbfile)
    logging.info("Loaded %d movies from database=%s",
                 len(db.db), options.dbfile)
    totalfiles = 0
    hashesadded = 0

    if options.search:
        results = db.search(options.search.lower())
        if len(results) > 0:
            printmovies(results)
            if not options.scan:
                sys.exit(0)
        

    for basepath, dirs, files in os.walk( os.path.abspath(options.startdir) ):
        for filename in files:

            if options.search:
                if options.search.lower() not in filename.lower():
                    continue

            video = basepath + "/" + filename
            extension = filename.split('.')[-1]
            basename = '.'.join(filename.split('.')[0:-1])
            myfilesize = GetHumanReadable(os.path.getsize(video),0)

            myvideodir=basepath
            myvideoyear='n/a'
            myvideoname=filename
            mygenre = 'n/a'

            try:
                myvideodir = basepath.split('/')[-1]
                myvideoyear = myvideodir.split('.')[-1]
                myvideoname = ".".join(myvideodir.split('.')[0:-1])
                mygenre = basepath.split('/')[-2]
            except Exception as e:
                logging.error("Error parsing video names: %s", e)

            if filename.lower().endswith(('.mkv','.avi','.mp4','.mpeg')):
                totalfiles += 1
                logging.debug('Found (%s)',video)
                logging.debug('BasePath (%s), Filename (%s), Basename (%s), Extension (%s)',basepath,filename,basename,extension)

                md5value = None
                hashfile = basepath + "/" + basename + ".md5"
                if os.path.isfile(hashfile):
                    logging.debug('Found MD5 hash existing for (%s)!', filename)
                    chkfh = open(hashfile,'r')
                    md5value = chkfh.readline().split()[0].lower()
                    chkfh.close()
                    if options.checkvideos:
                        logging.debug('Existing hash for video (%s) is (%s)',video,filevalue)
                        checkvalue = md5Checksum(video).lower()
                        if md5value != checkvalue:
                            logging.error('BAD: Hash does not match for file (%s), stored hash (%s), computed hash (%s)!',
                                          video, md5value, checkvalue)
                        else:
                            logging.info('GOOD: %s [ %s / %s ]', video, md5value,
                                         checkvalue)
                    
                else:
                    logging.info('Generating hash for (%s)',filename)
                    md5value = md5Checksum(video)
                    hashfh = open(hashfile,'w')
                    hashfh.write(md5value + "\t" + filename)
                    hashfh.close()
                    logging.info('Wrote computed value (%s) for filename (%s)',md5value,filename)
                    hashesadded += 1

                mkvinfo = None
                if 'mkv' in extension.lower():
                    mkvinfo = _mkvinfo(video)

                db.add(title=myvideoname, year=myvideoyear, genre=mygenre,
                       filename=video, md5sum=md5value,
                       filetype=extension.lower(), filesize=myfilesize,
                       mkvinfo=mkvinfo)

                if options.search:
                    printmovies([db.db[md5value]])

    if not options.search:
        logging.info('Completed (%d) files, added (%d) hashes!',totalfiles,hashesadded)

    db.save()
    return

if __name__ == '__main__':
    usage = "Usage: %prog [options] arg"
    parser = optparse.OptionParser(usage, version="%prog 1.0")
    parser.add_option('-s','--search', dest='search', type='string',help='Search string [%default]',default=None)
    parser.add_option('--db', dest='dbfile', type='string', help='Database file [%default]', default="/d1/movies/db.json")
    parser.add_option('-d','--start-dir',dest='startdir', type='string',metavar='STARTDIR',help='Start Directory to start processing movies [%default]',default='/d1/movies/')
    parser.add_option("-l", "--log-level", dest="log_level", type='string',metavar='LEVEL',help="change log level [%default]",default='info')
    parser.add_option("-c", "--check-videos", dest="checkvideos", action="store_true",help="Check video MD5's to find bad ones [%default]",default=False)
    parser.add_option("--scan", dest="scan", action="store_true", help="Scan files in addition to search db [%default]", default=False)
    parser.add_option("--continuous", dest="loop", type='int', help="Run continuously, and loop every [%default] seconds",default=0)
    group = optparse.OptionGroup(parser, "Debug Options")
    group.add_option("--debug", action="store_true",help="Print debug information")
    parser.add_option_group(group)
    (options, args) = parser.parse_args()

    logger = logging.getLogger('')
    level = options.log_level.upper()
    logger.setLevel(getattr(logging, level))
    stderr_handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(process)d] %(name)s - %(levelname)s - %(message)s")
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)

    enzyme_logger = logging.getLogger("enzyme")
    enzyme_logger.setLevel(logging.ERROR)

    main(options)
    exit(0)
