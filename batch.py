import httplib2
import os
import json
import arrow
import pymongo
import sys
import time
import random

from apiclient import discovery
from apiclient.http import BatchHttpRequest
import oauth2client
from oauth2client import client
from oauth2client import tools

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

SCOPES = 'https://www.googleapis.com/auth/drive.readonly'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Other Client 1'

batchset = set()
backoff = 0

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'drive-batch.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:  # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def _safe_print(u, errors="replace"):
    """Safely print the given string.

    If you want to see the code points for unprintable characters then you
    can use `errors="xmlcharrefreplace"`.
    """
    s = u.encode(sys.stdout.encoding or "utf-8", errors)
    print(s)


def store_metadata(request_id, response, exception):
    global batchset, backoff
    if exception is not None:
        if exception.resp.status == 403:
            backoff += 1
            print("Exception 403:  back off set to {}".format(backoff))  # TODO Add exponential back off here
        else:
            raise()
    else:
        print(len(batchset), arrow.now().timestamp, request_id, response)
        # if 'imageMediaMetadata' in response:
        #     response['date'] = response['imageMediaMetadata']['date']
        #     del response['imageMediaMetadata']
        # else:
        #     response['date'] = None
        # _safe_print(response['title'])
        # db.insert(response)
        # added += 1
        try:
            batchset.remove(request_id)
        except:
            print("Trouble removing {} from batch set".format(request_id))


# Receive MD5 list
# Update database since last check
# Search for MD5s in database
# Return True/False, and for True file names and path(?)

#Got root id using files.list with id 'root'
#'0AKq0_TYoRIPnUk9PVA' in parents and mimeType = 'application/vnd.google-apps.folder' and name = 'Google Photos'
#Also can get 'spaces':  GET https://www.googleapis.com/drive/v3/files?corpus=user&q='0AKq0_TYoRIPnUk9PVA'+in+parents+and+mimeType+%3D+'application%2Fvnd.google-apps.folder'+and+name+%3D+'Google+Photos'&fields=files(id%2Cname%2Cspaces)&key={YOUR_API_KEY}
#Actually just search over spaces=photos

def main():
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)
    db = pymongo.MongoClient().gp.gp_photos
    # Connect to MongoDB


# Get file list from Google Drive
    if False:
        file_list = None
        indexed = 0
        skipped = 0
        added = 0
        start = arrow.now()
        while True:
            if file_list is None:
                print("Getting first list", end="...")
                file_list = service.files().list(q="(mimeType contains 'image/' or mimeType contains 'video/')", maxResults=1000, fields="items/id, nextPageToken").execute()
                print("Done getting first list")
            else:
                if 'nextPageToken' not in file_list:
                    break
                file_list = service.files().list(pageToken=file_list['nextPageToken'], maxResults=1000, fields="items/id, nextPageToken").execute()
            print("Putting in database")
            for f in file_list['items']:
                indexed += 1
                if db.find_one({'id': f['id']}) is None:
                    db.insert({'id': f['id']})
                    added += 1
                else:
                    # print "Id already in database"
                    skipped += 1
                if not (indexed % 100 < 0.1):
                    print("Elapsed: {} Indexed: {}, Added: {}, Skipped: {}".format(arrow.now() - start, indexed, added, skipped))
        print("***Done*** Elapsed: {} Indexed: {}, Added: {}, Skipped: {}".format(arrow.now() - start, indexed, added, skipped))

    #Get metadata for each file
    if False:

        start = arrow.now().float_timestamp
        traversed = 0
        for target in db.find():  #TODO this should skip records that have already been retreived
            traversed += 1
            answer = service.files().get(fileId=target['id'], fields="createdDate, fileSize, id, imageMediaMetadata, indexableText, kind, md5Checksum, mimeType, originalFilename, parents, spaces, thumbnail, thumbnailLink, title").execute()
            db.replace_one({'id': answer['id']}, answer)
            if not traversed % 100:
                elapsed = arrow.now().float_timestamp - start
                print("{}: {} {}/sec".format(elapsed, traversed, traversed/elapsed))
    #    print(json.dumps(answer, sort_keys = True, indent = 4))
    #    time.sleep((2 ** backoff) + random.randint(0, 1000) / 1000)
    #    batch = service.new_batch_http_request()
    #    batch.add(service.files().get(fileId=f['id'], fields="title, mimeType, id, md5Checksum, imageMediaMetadata/date, fileSize"), callback=store_metadata, request_id=f['id'])
    #    batchset.add(f['id'])
    #    batch.execute(http=http)
    # #    print("Traversed: {}, Added: {}, Skipped: {}, Elapsed time: {}, Remaining in batchset: {}".format(traversed, added, skipped, arrow.now() - start, batchset))

        print( "***Done***")

#db.getCollection('gp_batch').aggregate({$group: {_id: "$md5Checksum", count:{$sum: 1}}},{$match: {count: {$gt: 1}}} )   #Example that find md5 duplicates
    #Get Parent directory info




if __name__ == '__main__':
    main()
