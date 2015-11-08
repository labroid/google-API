__author__ = 'scott_jackson'
import httplib2
import os
import json
import arrow
import pymongo
import sys

from apiclient import discovery
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
                                   'drive-quickstart.json')

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

def main():
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v2', http=http)

    #Connect to MongoDB
    db = pymongo.MongoClient().gp.gp_collection

    file_list = None
    traversed = 0
    added = 0
    skipped = 0
    start = arrow.now()
    while True:
        if file_list is None:
            file_list = service.files().list(q="(mimeType contains 'image/' or mimeType contains 'video/')", maxResults=1, fields="items/id, nextPageToken").execute()
        else:
            if 'nextPageToken' not in file_list:
                break
            file_list = service.files().list(pageToken=file_list['nextPageToken'], maxResults=1000, fields="items/id, nextPageToken").execute()

        for f in file_list['items']:
            traversed += 1
            if db.find_one({'id':f['id']}) is None:
                #might need to add exponential backoff on error 503
                md = service.files().get(fileId=f['id'], fields="title, mimeType, id, md5Checksum, imageMediaMetadata/date, fileSize").execute()
                if 'imageMediaMetadata' in md:
                    md['date'] = md['imageMediaMetadata']['date']
                    del md['imageMediaMetadata']
                else:
                    md['date'] = None
                _safe_print(md['title'])
                db.insert(md)
                added += 1
            else:
                #print "Id already in database"
                skipped += 1

        print "Traversed: {}, Added: {}, Skipped: {}, Elapsed time: {}".format(traversed, added, skipped, arrow.now() - start)

    print "***Done***"


if __name__ == '__main__':
    main()