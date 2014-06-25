#!/usr/bin/env python

import os,fnmatch,time
import optparse,logging
import hashlib

def md5Checksum(filePath):
    with open(filePath, 'rb') as fh:
        m = hashlib.md5()
        while True:
            data = fh.read(8192)
            if not data:
                break
            m.update(data)
        return m.hexdigest()

def locate(pattern, root=os.curdir):
    '''Locate all files matching supplied filename pattern in and below
    supplied root directory.'''
    for path, dirs, files in os.walk(os.path.abspath(root)):
        for filename in fnmatch.filter(files, pattern):
            yield os.path.join(path, filename)

def main(options):
    totalfiles = 0
    hashesadded = 0
    for video in locate('*.mkv',options.startdir):
        totalfiles += 1
        basepath = os.path.dirname(video);
        filename = video.split('/')[-1]
        basename = '.'.join(filename.split('.')[0:-1])
        extension = filename.split('.')[-1]
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

    logging.info('Completed (%d) files, added (%d) hashes!',totalfiles,hashesadded)
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
    parser.add_option('-s','--start-dir',dest='startdir',type='string',metavar='STARTDIR',help='Start Directory to start processing movies [%default]',default='/d1/movies/')
    parser.add_option("-l", "--log-level", dest="log_level", type='string',metavar='LEVEL',help="change log level [%default]",default='info')
    parser.add_option("-c", "--check-videos", dest="checkvideos", action="store_true",help="Check video MD5's to find bad ones [%default]",default=False)
    parser.add_option("--continuous", dest="loop", type='int', help="Run continuously, and loop every [%default] seconds",default=0)
    group = optparse.OptionGroup(parser, "Debug Options")
    group.add_option("-d", "--debug", action="store_true",help="Print debug information")
    parser.add_option_group(group)

    (options, args) = parser.parse_args()

    logger = logging.getLogger('')
    level = options.log_level.upper()
    logger.setLevel(getattr(logging, level))
    stderr_handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(process)d] %(name)s - %(levelname)s - %(message)s")
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)

    if options.loop > 0:
        while True:
            logging.info('Running in continuous mode every (%d) seconds',options.loop)
            main(options)
            time.sleep(options.loop)
    else:
        main(options)
        exit(0)
