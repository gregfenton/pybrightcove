#!/usr/bin/python

#------------------------------------------------------------------------------
# This script takes in a list of filenames and processes them.
# The files are lists of video IDs, one ID per line.
# For each ID, it downloads the video and its metadata from the
# old (source) account and then uploads to the new (destination)
# account.
# The script also generates a CSV file containing rows of
# the old video ID and the corresponding new video ID.
#
# Usage:
#   python copy_videos.py <video_ids_file1> [<video_ids_file2> ...]
#
# Example: (suggestion: run in a new virtualenv)
#   $ pip install pybrightcove
#   ...
#
#   $ vi copy_videos.py
#       ### set the values in the GLOBALS section to your accounts' API keys
#
#   $ python copy_videos.py list1.txt
#   [2014-11-16 13:53:12] INFO    Start
#   [2014-11-16 13:53:12] INFO    Starting (list1.txt)
#   [2014-11-16 13:53:12] INFO    Recording old_id,new_id to
#      (./old_new_ids-list1.txt.csv)
#   [2014-11-16 13:53:12] INFO    Video(3450610395001)
#   [2014-11-16 13:53:12] INFO      fetching ID(3450610395001) codec(H264) to
#      (./downloads/3450610395001.mp4)
#   [2014-11-16 13:53:14] INFO    Video(3405747632001)
#   [2014-11-16 13:53:15] INFO      fetching ID(3405747632001) codec(H264) to
#      (./downloads/3405747632001.mp4)
#   [2014-11-16 13:53:19] INFO    Done.
#
#   $ ls downloads
#   3405747632001.mp4  3450610395001.mp4
#
#   $ cat old_new_ids-list1.txt.csv
#   "old_id","new_id"
#   "3550710396001","3894189769001"
#   "3605748630001","3894183053001"
#
#------------------------------------------------------------------------------

from pybrightcove.connection import APIConnection
from pybrightcove.exceptions import NoDataFoundError, IllegalValueError
from pybrightcove.video import Video

import errno
import logging
import os
import sys
import urllib2

#############
#     GLOBALS: you need to set your accounts' API keys
#
# - API key from old (source) account
OLD_READ_TOKEN=''
# - API keys from new (destination) account
NEW_READ_TOKEN = ''
NEW_WRITE_TOKEN = ''
# - custom fields you want to migrate
MY_CUSTOM_FIELDS = 'field_1,field_2'
# - directory where video files will be downloaded to (then uploaded from)
DOWNLOAD_DIR = './downloads/'
#
#
#############

OLDCONN = APIConnection(read_token=OLD_READ_TOKEN)
NEWCONN = APIConnection(read_token=NEW_READ_TOKEN, write_token=NEW_WRITE_TOKEN)

def process_videos_list(fname):
  if os.path.isfile(fname):
    lines = [line.strip() for line in open(fname)]
  else:
    logger.warn("File not found! (%s)" % (fname,))
    return

  logger.info("Starting (%s)" % name)
  
  csvfname = './old_new_ids-' + os.path.basename(fname) + '.csv'
  if os.path.isfile(csvfname):
    logger.critical("CSV file already exists! (%s).  Exiting." % (csvfname,))
    sys.exit(1)
  
  csv = open(csvfname, "w")
  logger.info("Recording old_id,new_id to (%s)" % (csvfname,))
  csv.write("\"old_id\",\"new_id\"\n")

  for oldId in lines:
    v = download_video(oldId)
    if v is not None:
      v = upload_video(v)
      if v is not None:
        record_old_new_ids(csv, oldId, v.id)
        v.id = oldId

  csv.close()

def download_video(id):
  logger.info("Video(%s)" % (id,))
  try:
      # get video object from ID
      v = Video(id=id, _connection=OLDCONN, media_delivery="http",
                custom_fields=MY_CUSTOM_FIELDS)
  except NoDataFoundError:
      logger.warn("  **> No data found for (%s)" % (id,))
      return None

  # determine best rendition (largest size)
  best_rendition = None
  for rendition in v.renditions:
    if rendition.url is not None and rendition.url.startswith('http'):
      if best_rendition is None:
        best_rendition = rendition
      elif rendition.size > best_rendition.size:
        best_rendition = rendition

  if best_rendition is None:
    logger.warn("  No best rendition found for (%s). Skipping." % (id,))
    return
  else:
    # use best rendition to fetch video
    fname = DOWNLOAD_DIR + str(id)
    if best_rendition.video_codec == 'H264':
        fname += ".mp4"
    elif best_rendition.video_codec == 'M2TS':
        fname += ".m2ts"
    logger.info("  fetching ID(%s) codec(%s) to (%s)" %
                (v.id, best_rendition.video_codec,fname,))

    try:
      req = urllib2.urlopen(best_rendition.url)
      downloaded = 0
      CHUNK = 4096
      with open(fname, 'wb') as fp:
        while True:
          chunk = req.read(CHUNK)
          downloaded += len(chunk)
          if not chunk: break
          fp.write(chunk)
      logger.debug("  bytes downloaded: " + str(downloaded))
      v._filename = fname  # required for upload with v.save()
    except urllib2.HTTPError, e:
      logger.warn("HTTP Error: %s - (%s)" % (e.code, url,))
      return False
    except urllib2.URLError, e:
      logger.warn("URL Error: %s - (%s)" % (e.reason, url,))
      return False
  return v

def upload_video(v):
  v.connection = NEWCONN
  v.id = None
  v.renditions = None      # renditions will be generated in the new account

  try:
    v.save()
  except IllegalValueError, e:
    logger.warn("  Upload error: (%s) - %s" % (e.code, e.description, ))
    v = None
  return v

def record_old_new_ids(fh, oldid, newid):
  fh.write("\"%s\",\"%s\"\n" % (str(oldid), str(newid),))
  return

###
### Main
###
logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] %(levelname)-7s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

logger.info("Start")

try:
  os.makedirs(DOWNLOAD_DIR)
except OSError as exception:
  if exception.errno != errno.EEXIST:
    raise

# process the files of video ID lists (1 ID per line)
for name in sys.argv[1:]:
  process_videos_list(name)

logger.info('Done.')
