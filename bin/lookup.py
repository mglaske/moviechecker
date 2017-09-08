#!/usr/bin/env python

import os,sys,fnmatch,time
import optparse,logging
import enzyme
import hashlib
#from hachoir_core.cmd_line import unicodeFilename
#from hachoir_core.i18n import getTerminalCharset
#from hachoir_metadata import extractMetadata
#from hachoir_parser import createParser

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
    

def main(options):
    totalfiles = 0
    hashesadded = 0
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
            except Exception, e:
                logging.error("Error parsing video names: %s",e)
                pass

            if filename.lower().endswith(('.mkv','.avi','.mp4','.mpeg')):
                totalfiles += 1
                logging.debug('Found (%s)',video)
                logging.debug('BasePath (%s), Filename (%s), Basename (%s), Extension (%s)',basepath,filename,basename,extension)

                hashfile = basepath + "/" + basename + ".md5"

                if os.path.isfile(hashfile):
                    logging.debug('Found MD5 hash existing for (%s)!', filename)
                    if options.checkvideos:
                        chkfh = open(hashfile,'r')
                        filevalue = chkfh.readline().split()[0].lower()
                        chkfh.close()
                        logging.debug('Existing hash for video (%s) is (%s)',video,filevalue)
                        md5value = md5Checksum(video).lower()
                        if md5value != filevalue:
                            logging.error('BAD: Hash does not match for file (%s), stored hash (%s), computed hash (%s)!',video,filevalue,md5value)
                        else:
                            logging.info('GOOD: %s [ %s / %s ]',video,filevalue,md5value)
                    
                else:
                    logging.info('Generating hash for (%s)',filename)
                    md5value = md5Checksum(video)
                    hashfh = open(hashfile,'w')
                    hashfh.write(md5value + "\t" + filename)
                    hashfh.close()
                    logging.info('Wrote computed value (%s) for filename (%s)',md5value,filename)
                    hashesadded += 1

                if options.search:
                    sys.stdout.write( "{0:<12s}  {1:<50s}  {2:<5s}".format(mygenre,myvideoname, myvideoyear) )

                    if 'mkv' in extension:
                        mkvinfo = _mkvinfo(video)
                        duration = str(mkvinfo['duration']).split('.')[0]
                        res = "%s (%s)" % ( mkvinfo['video'][0]['resolution'],mkvinfo['video'][0]['resname'] )
                        sys.stdout.write( "  {0:<10s}  Type={1:<3s}  Resolution={2:<18s}  Audio_Channels={3:<2d}  Size={4:<15s}".format( duration, extension.upper(), res, mkvinfo['audio'][0]['channels'], myfilesize) )

                    sys.stdout.write("\n")

    if not options.search: logging.info('Completed (%d) files, added (%d) hashes!',totalfiles,hashesadded)
    return

if __name__ == '__main__':
    loglevels = {
        'WARNING' : logging.WARNING,
        'INFO' : logging.INFO,
        'ERROR' : logging.ERROR,
        'CRITICAL' : logging.CRITICAL,
        }

    usage = "Usage: %prog [options] arg"
    parser = optparse.OptionParser(usage, version="%prog 1.0")
    parser.add_option('-s','--search',dest='search',type='string',help='Search string [%default]',default=None)
    parser.add_option('-d','--start-dir',dest='startdir',type='string',metavar='STARTDIR',help='Start Directory to start processing movies [%default]',default='/d1/movies/')
    parser.add_option("-l", "--log-level", dest="log_level", type='string',metavar='LEVEL',help="change log level [%default]",default='info')
    parser.add_option("-c", "--check-videos", dest="checkvideos", action="store_true",help="Check video MD5's to find bad ones [%default]",default=False)
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
