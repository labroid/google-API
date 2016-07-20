import httplib2
import os
import pymongo
import sys
import logging

from apiclient import discovery
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


def _safe_print(u, errors="replace"):
    """Safely print the given string.

    If you want to see the code points for unprintable characters then you
    can use `errors="xmlcharrefreplace"`.
    """
    s = u.encode(sys.stdout.encoding or "utf-8", errors)
    print(s)


def main():
    logging.info("Start up")
    archive = Gphotos('gp', 'gphotos')
    archive.sync()
    logging.info("Done")
    print("***Done***")

#Gphotos API
#Gphotos(db)
#Gphotos.sync()
#Gphotos.check_path(path)
#Gphotos.check_member(MD5)
#Gphotos.stats()

class Gphotos(object):
    """
    Gphotos:  A set of tools to aid management of local images and a Google Photos repository
    """
    def __init__(self, database, collection):
        self.service = self.get_service()
        self.db = pymongo.MongoClient()[database][collection]
        self.db.create_index('id')


    def check_member(self, md5):
        return self.db.find_one({'md5Sum': md5})


    def sync(self):
        """
        Synchronize database with google photos
        """

        # TODO:  Make sure we don't 'find' files that are marked at trashed

        INIT_FIELDS = "files(id,imageMediaMetadata/time,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,spaces,explicitlyTrashed,trashed), nextPageToken"
        change_token_cursor = self.db.find({'change_token': {'$exists': True}})
        assert change_token_cursor.count() <= 1
        if change_token_cursor.count() == 0:  # If we have no change token, drop and resync the database
            logging.info("No change token available - resyncing database")
            self.db.drop()
            next_page_token = None
            while True:
                file_list = self.service.files().list(pageToken=next_page_token,
                                                 spaces='photos',
                                                 pageSize=1000,
                                                 fields=INIT_FIELDS).execute()
                if 'files' in file_list:
                    file_count = len(file_list['files'])
                else:
                    file_count = 0
                logging.info("Google sent {} records".format(file_count))
                db_status = self.db.insert_many(file_list.get('files'))
                logging.info("Mongodb stored {} records".format(len(db_status.inserted_ids)))
                assert file_count == len(
                    db_status.inserted_ids), "Records stored != records from gPhotos.  Got {} gPhotos and {} ids".format(
                    file_count, len(db_status.inserted_ids))
                if 'nextPageToken' in file_list:
                    next_page_token = file_list['nextPageToken']
                else:
                    break
            # Once db is updated with all changes, get initial change token
            change_token = self.service.changes().getStartPageToken().execute()
            self.db.insert({'change_token': change_token['startPageToken']})
        else:
            logging.info('Have change token; updating database.')
            change_token = change_token_cursor[0]['change_token']
            UPDATE_FIELDS = 'changes(file(id,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,spaces,explicitlyTrashed,trashed),fileId,removed,time),kind,newStartPageToken,nextPageToken'
            while True:
                changes = self.service.changes().list(pageToken=change_token,
                                                 spaces='photos',
                                                 pageSize=1000,
                                                 includeRemoved=True,
                                                 fields=UPDATE_FIELDS).execute()
                change_count = len(changes.get('changes', []))
                logging.info("Google sent {} records".format(change_count))
                if change_count:  # TODO:  If 'removed' is True then remove file from database:  changes['changes'][0]['removed']
                    for change in changes['changes']:
                        if change['removed'] is True:
                            db_status = self.db.delete_one({'id': change['fileId']})
                            assert db_status.deleted_count == 1, "Deleted files count should be 1, got {}".format(
                                db_status.deleted_count)
                        else:
                            db_status = self.db.replace_one({'id': change['file']['id']}, change['file'],
                                                       upsert=True)  # TODO:  Make sure the data that comes with change is complete for insertion
      #                      assert db_status.modified_count == 1, "Modified files count should be 1, got {}".format(
      #                          db_status.modified_count)
                if 'nextPageToken' in changes:
                    change_token = changes['nextPageToken']
                else:
                    assert 'newStartPageToken' in changes, "newStartPageToken missing when nextPageToken is missing.  Should never happen."
                    db_status = self.db.replace_one({'change_token': {'$exists': True}},
                                               {'change_token': changes['newStartPageToken']})
                    assert db_status.modified_count == 1, "Database did not update correctly"
                    break  # All changes have been received
        logging.info('Done with database resync')


        self.__get_parents()  # TODO:  Unfortuantely this and set_path runs even if there were no changes....
        # TODO:  What about photo deletes?
        root_id = self.service.files().list(q='name="Google Photos"').execute()['files'][0]['id']
        self.__set_paths(root_id, ['Google Photos'])
        logging.info('Done set_paths')

    def __get_parents(self):
        """
        Populate database entries for parent folders
        :return: None.  Changes database
        """
        # TODO:  This delivers a datbase record with "My Drive" in it.  That is too high in the tree.....

        parents_needed = set(self.db.distinct('parents'))  # Seed not_in_db_set with all parents assuming none are present
        ids_in_db = set(self.db.distinct('id'))
        parents_needed.difference_update(ids_in_db)
        while parents_needed:
            parent_id = parents_needed.pop()
            parent_meta = self.service.files().get(fileId=parent_id, fields='id,kind,md5Checksum,mimeType,name,ownedByMe,parents,size,trashed').execute()
            self.db.insert(parent_meta)  #TODO Check write was successful?
            ids_in_db.add(parent_id)
            for parent in parent_meta.get('parents') or []:
                if parent not in ids_in_db:
                    parents_needed.add(parent)
        logging.info('Done getting parents')


    def __set_paths(self, id, path):
        """
        Sets path ids for folders
        :param id: Google Drive id of Google Photos folder
        :param path: Google Drive path to file with Google Drive id
        :return: None. Adds path to each folder in Google Photos
        """
        children = self.db.find({'mimeType': 'application/vnd.google-apps.folder', 'parents': id})
        self.db.update_one({'id': id}, {'$set': {'path': path}})
        if children.count() != 0:
            for child in children:
                my_name = self.db.find_one({'id': child['id']})['name']
                path.append(my_name)
                self.__set_paths(child['id'], path)
                path.pop()

    def get_service(self):
        credentials = self.get_credentials()
        http = credentials.authorize(httplib2.Http())
        service = discovery.build('drive', 'v3', http=http)
        return service

    def get_credentials(self):
        """Gets valid user credentials from storage.

        If nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is completed to obtain the new credentials.

        Returns:
            Credentials, the obtained credential.
        """
        SCOPES = 'https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/drive.photos.readonly'
        CLIENT_SECRET_FILE = 'client_secret.json'
        APPLICATION_NAME = 'Other Client 1'

        try:
            import argparse
            flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
        except ImportError:
            flags = None

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


#db.getCollection('gp_batch').aggregate({$group: {_id: "$md5Checksum", count:{$sum: 1}}},{$match: {count: {$gt: 1}}} )   #Example that find md5 duplicates

if __name__ == '__main__':
    main()
