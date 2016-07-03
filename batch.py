import httplib2
import os
import json
import arrow
import pymongo
import sys
import logging
import time

from apiclient import discovery
from apiclient.http import BatchHttpRequest
import oauth2client
from oauth2client import client
from oauth2client import tools

# TODO:  Is is possible to log to a database?  See log4mongo
# TODO:  Also see Google API Client Libraries Python Logging
#log_file = os.path.join(r"C:\Users\SJackson\Documents\Personal\Programming", time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()) + ".txt")
log_file = os.path.join(r"C:\Users\SJackson\Documents\Personal\Programming\photolog.txt")
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
logging.basicConfig(
    filename=log_file,
    format=LOG_FORMAT,
    level=logging.DEBUG,
    filemode='w'
)



try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

#SCOPES = 'https://www.googleapis.com/auth/drive.photos.readonly'
SCOPES = 'https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/drive.photos.readonly'
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


# Receive MD5 list
# Update database since last check
# Search for MD5s in database
# Return True/False, and for True file names and path(?)

#Got root id using files.list with id 'root'
#'0AKq0_TYoRIPnUk9PVA' in parents and mimeType = 'application/vnd.google-apps.folder' and name = 'Google Photos'
#Also can get 'spaces':  GET https://www.googleapis.com/drive/v3/files?corpus=user&q='0AKq0_TYoRIPnUk9PVA'+in+parents+and+mimeType+%3D+'application%2Fvnd.google-apps.folder'+and+name+%3D+'Google+Photos'&fields=files(id%2Cname%2Cspaces)&key={YOUR_API_KEY}
#Actually just search over spaces=photos


def main():
    check_in_gphotos(None)
    print("***Done***")


def check_in_gphotos(md5list):
    #Set up service, authenticating as necessary
    service = get_service()
    # Connect to MongoDB
    db = pymongo.MongoClient().gp.gphotos
    db.create_index('id')
    sync_db(db, service)
    get_parents(db, service)


def sync_db(db, service):
    """
    Synchronize database with google photos
    :param db: mongodb collection
    :param service: service object authenticated to google photo collection
    :return: None - side effect is updated database collection db
    """

    INIT_FIELDS = "files(id,imageMediaMetadata/time,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,spaces), nextPageToken"
    change_token_cursor = db.find({'change_token': {'$exists': True}})
    assert change_token_cursor.count() <= 1
    if change_token_cursor.count() == 0:  # If we have no change token, drop and resync the database
        logging.info("No change token available - resyncing database")
        db.drop()
        next_page_token = None
        while True:
            file_list = service.files().list(pageToken=next_page_token,
                                             spaces='photos',
                                             pageSize=1000,
                                             fields=INIT_FIELDS).execute()
            if 'files' in file_list:
                file_count = len(file_list['files'])
            else:
                file_count = 0
            logging.info("Google sent {} records".format(file_count))
            db_status = db.insert_many(file_list.get('files'))
            logging.info("Mongodb stored {} records".format(len(db_status.inserted_ids)))
            assert file_count == len(db_status.inserted_ids), "Records stored != records from gPhotos.  Got {} gPhotos and {} ids".format(file_count, len(db_status.inserted_ids))
            if 'nextPageToken' in file_list:
                next_page_token = file_list['nextPageToken']
            else:
                break
        # Once db is updated with all changes, get initial change token
        change_token = service.changes().getStartPageToken().execute()
        db.insert({'change_token': change_token['startPageToken']})
        # TODO:  Get the parents
        logging.info('Done database resync')

    else:
        logging.info('Have change token; updating database.')
        change_token = change_token_cursor[0]['change_token']
        UPDATE_FIELDS = 'changes(file(id,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,spaces)),kind,newStartPageToken,nextPageToken'
        while True:
            changes = service.changes().list(pageToken=change_token,
                                             spaces='photos',
                                             pageSize=1000,
                                             includeRemoved=True,
                                             fields=UPDATE_FIELDS).execute()
            change_count = len(changes.get('changes', [0]))
            logging.info("Google sent {} records".format(change_count))
            if change_count:
                db_status = db.insert_many(changes['changes'])
                logging.info("Mongodb stored {} records".format(len(db_status.inserted_ids)))
                assert change_count == len(db_status.inserted_ids), "Records stored != records from gPhotos.  Got {} gPhotos and {} ids".format(change_count, len(db_status.inserted_ids))
            if 'nextPageToken' in changes:
                change_token = changes['nextPageToken']
            else:
                assert 'newStartPageToken' in changes, "newStartPageToken missing when nextPageToken is missing.  Should never happen."
                db_status = db.replace_one({'change_token': {'$exists': True}}, {'change_token': changes['newStartPageToken']})
                assert db_status.modified_count == 1, "Database did not update correctly"
                break  #All changes have been received
        logging.info('Done database update')

    get_parents(db, service)
    # TODO:  What about page deletes?

def get_parents(db, service):
    parents_needed = set(db.distinct('parents'))  #Seed not_in_db_set with all parents assuming none are present
    ids_in_db = set(db.distinct('id'))
    parents_needed.difference_update(ids_in_db)
    while parents_needed:
        parent_id = parents_needed.pop()
        parent_meta = service.files().get(fileId=parent_id, fields='id,kind,md5Checksum,mimeType,name,ownedByMe,parents,size,trashed').execute()
        logging.info("Parent node found: {}".format(parent_meta['name']))
        db.insert(parent_meta)  #TODO Check write was successful?
        ids_in_db.add(parent_id)
        for parent in parent_meta.get('parents') or []:
            if parent not in ids_in_db:
                parents_needed.add(parent)

indexed = 0
added = 0
skipped = 0

def update_db(db, file_list):
    global indexed, added, skipped
    print("Putting in database")
    for f in file_list:
        indexed += 1
        if db.find_one({'id': f['id']}) is None:
            db.insert(f)
            added += 1
        else:
            skipped += 1
        if not indexed % 100:
            print("Indexed: {}, Added: {}, Skipped: {}".format(indexed, added, skipped))
    print("***Done*** Indexed: {}, Added: {}, Skipped: {}".format(indexed, added, skipped))


#db.getCollection('gp_batch').aggregate({$group: {_id: "$md5Checksum", count:{$sum: 1}}},{$match: {count: {$gt: 1}}} )   #Example that find md5 duplicates
    #Get Parent directory info


def get_service():
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)
    return service


if __name__ == '__main__':
    main()
