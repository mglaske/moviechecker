#!/usr/bin/env python
import os
import logging
import hashlib
from pymediainfo import MediaInfo
import helpers


class MediaFile():

    def __init__(self, path):
        self.path = path
        self.filename = os.path.basename(path)
        self.md5 = self.md5file(generate_missing=False)
        self.md5file = None      # Only the md5 value retrieved from the file
        self.md5computed = None  # If we computed a hash, this is the value.
        self.log = logging.getLogger()

    def md5filename(self):
        splits = self.path.split('.')
        base = ".".join(splits[0:-1])
        md5file = base + ".md5"
        return md5file

    def md5file(self, generate_missing=True):
        # given a path, pull the md5 from the file
        md5file = self.md5filename()
        if os.path.isfile(md5file):
            try:
                with open(md5file, 'r') as fh:
                    md5value = fh.readline().split()[0].lower()
                self.md5 = md5value
                self.md5file = md5value
                return md5value
            except Exception as e:
                self.log.error("Unable to get md5file=%s: %s", md5file, e)
        elif generate_missing:
            # Generate missing md5 file.
            return self.generate_checksum()
        return None

    def md5Checksum(self):
        try:
            m = hashlib.md5()
            with open(self.path, 'rb') as fh:
                while True:
                    data = fh.read(8192)
                    if not data:
                        break
                    m.update(data)
            self.md5computed = m.hexdigest()
            return self.md5computed
        except Exception as e:
            self.log.error("Unable to compute checksum of (%s): %s", self.path, e)
        return None

    def generate_checksum(self):
        self.log.info('Generating hash for (%s)', self.filename)
        md5value = self.md5Checksum()
        if not md5value:
            return None
        self.md5 = md5value
        md5file = self.md5filename()
        try:
            with open(md5file, 'w') as fh:
                fh.write(md5value + "\t" + filename)
            self.log.info('Wrote computed value (%s) for filename (%s)',
                          md5value, os.path.basename(md5file))
        except Exception as e:
            self.log.error("Unable to write checksum file (%s): %s",
                           md5file, e)
        return md5value

    def check_checksum(self):
        filesum = self.md5file(generate_missing=False)
        if not filesum:
            return None
        current = self.md5Checksum()
        if not current:
            return None
        return filesum.lower() == current.lower()

    def mediainfo(self):
        info = {'title': None, 'duration': None, 'chapters': None, 'video': [], 'audio': []}
        video = {'height': None, 'width': None, 'resolution': None, 'resname': None, 'codec': None, 'duration': None, 'bit_rate': None, 'bit_depth': None, 'aspect_ratio': None, 'color_primaries': None}
        audio = {'freq': None, 'channels': None, 'language': None, 'bit_depth': None, 'codec': None}
        try:
            mi = MediaInfo.parse(self.path)
        except Exception as e:
            self.log.error("MediaInfo threw error reading path=%s: %s", self.path, e)
            return info

        for t in mi.tracks:
            if t.track_type == 'General':
                info['duration'] = helpers.ms_to_human(t.duration or 0)
                continue
            if t.track_type == "Video":
                vt = dict(video)
                vd = t.to_data()
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
                vt['color_primaries'] = vd.get("color_primaries", "--")
                if t.bit_rate:
                    br = t.bit_rate
                elif t.nominal_bit_rate:
                    br = t.nominal_bit_rate
                else:
                    br = None
                try:
                    vt['bit_rate'] = helpers.speed_to_human(br)
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
