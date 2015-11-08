__author__ = 'scott_jackson'
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
        else: # Needed only for compatability with Python 2.6
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
        #print type(exception)
        #print vars(exception)
        #print "Whoa!  Exception!"
        if exception.resp.status == 403:
            backoff += 1
            print "Exception 403:  back off set to {}".format(backoff)  #Add exponential back off here
        else:
            raise
    else:
        print len(batchset), arrow.now().timestamp, request_id, response
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
            print "Trouble removing {} from batch set".format(request_id)


def main():
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
#    service = discovery.build('drive', 'v2', http=http)
    service = discovery.build('drive', 'v2', http=http)



    #Connect to MongoDB
    db = pymongo.MongoClient().gp.gp_collection

    file_list = None
    traversed = 0
    added = 0
    skipped = 0
    start = arrow.now()
    while True:
        batch = service.new_batch_http_request()
        time.sleep((2 ** backoff) + random.randint(0, 1000) / 1000)
        if file_list is None:
            file_list = service.files().list(q="(mimeType contains 'image/' or mimeType contains 'video/')", maxResults=1000, fields="items/id, nextPageToken").execute()
        else:
            if 'nextPageToken' not in file_list:
                break
            file_list = service.files().list(pageToken=file_list['nextPageToken'], maxResults=1000, fields="items/id, nextPageToken").execute()

        for f in file_list['items']:
            traversed += 1
#            if db.find_one({'id':f['id']}) is None:
            if True:
                batch.add(service.files().get(fileId=f['id'], fields="title, mimeType, id, md5Checksum, imageMediaMetadata/date, fileSize"), callback=store_metadata, request_id=f['id'])
                batchset.add(f['id'])

            else:
                #print "Id already in database"
                skipped += 1
        batch.execute(http=http)
        print "Traversed: {}, Added: {}, Skipped: {}, Elapsed time: {}, Remaining in batchset: {}".format(traversed, added, skipped, arrow.now() - start, batchset)

    print "***Done***"


if __name__ == '__main__':
    main()