import httplib2
import os
import pymongo
import sys
import logging
import hashlib
import shutil
import re
import time
import collections

from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools

LOCAL_ARCHIVE = r"E:\mnt"
GPHOTO_UPLOAD_QUEUE = r"C:\Users\SJackson\Pictures\GooglePhotosQueue"
IMAGE_FILE_EXTENSIONS = ['jpg']

LOG_FILE = os.path.join(r"C:\Users\SJackson\Documents\Personal\Programming", time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime()) + "photolog.txt")
#LOG_FILE = os.path.join(r"C:\Users\SJackson\Documents\Personal\Programming\photolog.txt")
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
logging.basicConfig(
    filename=LOG_FILE,
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

#confirmed_db = pymongo.MongoClient()['gp']['confirmed']

def main():
    logging.info("Start up")
    gphotos = Gphotos('gp', 'gphotos')
    archive = Local_archive(LOCAL_ARCHIVE, 'gp', 'archive')
    delete_queue(GPHOTO_UPLOAD_QUEUE)
    print("Syncing database")
    gphotos.sync()
    archive.sync_fs()
    archive.populate_md5s()
    cycle = 0
    queue_count = 0
    for photopath in archive.yield_missing(gphotos.db):
        queue_count += 1
        print('{}: copy {} to upload'.format(queue_count, photopath))
        try:
            shutil.copy2(photopath, GPHOTO_UPLOAD_QUEUE)
        except shutil.SameFileError:
            print("Same filename: {}".format(photopath))
            queue_count -= 1
        except:
            print("Some kind of problem trying to copy: {}".format(photopath))
            queue_count -= 1
        if queue_count >= 500:
            cycle += 1
            wait_for_backup(cycle)
            delete_queue(GPHOTO_UPLOAD_QUEUE)
            print("Syncing database")
            gphotos.sync()
            queue_count = 0
    cycle += 1
    wait_for_backup(cycle)
    delete_queue(GPHOTO_UPLOAD_QUEUE)
    print("Syncing database")
    gphotos.sync()
    logging.info("Done")
    print("***Done***")


def delete_queue(path):
    print("Deleting all files in Queue")
    for filename in os.listdir(path):
        try:
            os.remove(os.path.join(path, filename))
        except os.error as e:
            logging.error(e)

# This is here in case Google Photo Backup starts hanging and we need to restart - mostly to capture library name
# def restart_photo_backup():
#     import psutil
#
#     for proc in psutil.process_iter():
#         try:
#             pinfo = proc.as_dict(attrs=['pid', 'name'])
#         except psutil.NoSuchProcess:
#             pass
#         else:
#             print(pinfo)

def wait_for_backup(cycle):  # TODO:  Not sure how this works on an empty file or when it runs to top without finding token
    TIMEOUT = 20 * 60  #Twenty minutes
    GOOGLE_BACKUP_LOG = r"C:\Users\SJackson\AppData\Local\Google\Google Photos Backup\network.log"
    regex_count = re.compile('.*remainingMediaCount=([0-9]+).*')
    time_start = time.time()
    time_interval = time.time()
    last_remaining_count = -1
    while True:
        with open(GOOGLE_BACKUP_LOG) as f:
            tail = collections.deque(f)
            while True:
                target_count = tail.pop()
                found = regex_count.search(target_count)
                if found:
                    remaining_count = int(found.group(1))
                    break
        if remaining_count == 0 and last_remaining_count == -1:  #Log file hasn't updated yet - wait for remaining_count > 0
            print("Remaining count is 0; waiting for update")
        else:
            if remaining_count != last_remaining_count:
                elapsed = time.time() - time_start
                interval = time.time() - time_interval
                print("{} Elapsed: {:d}:{:02d} Interval: {:d}:{:02d} Cycle {}: {} remaining.".format(time.asctime(), int(interval//60), int(interval % 60), int(elapsed//60), int(elapsed%60), cycle, remaining_count))
                last_remaining_count = remaining_count
                time_start = time.time()  #restart timeout
            if remaining_count == 0:
                return
            if time.time() - time_start > TIMEOUT:
                print("{} Timed out".format(time.asctime()))
                logging.info("Timed out")
                return
        time.sleep(5)

class Local_archive(object):

    def __init__(self, top, database, collection):
        self.db = pymongo.MongoClient()[database][collection]
        self.db.create_index('md5')
        self.db.create_index('path')
        self.top = top

    def sync_fs(self):
        logging.info('Traversing tree at {} and storing paths.'.format(self.top))
        bulk_paths = []
        save_count = 0
        for root, dirs, files in os.walk(self.top):  #TODO:  Add error trapping argument
            for path in [os.path.join(root, x) for x in files]:
                if os.path.splitext(path)[1].lower() == '.jpg':
                    if not self.db.find_one({'path': path}):  #Skip if already in database
                        bulk_paths.append({'path': path})
                        save_count += 1
                if save_count >= 1000:
                    logging.info("Inserting at count = {}".format(save_count))
                    multi_status = self.db.insert_many(bulk_paths)
                    logging.info("dB insert status: {}".format(multi_status))
                    bulk_paths = []
                    save_count = 0
        if save_count:
            logging.info("Inserting at count = {}".format(save_count))
            multi_status = self.db.insert_many(bulk_paths)
            logging.info("dB insert status: {}".format(multi_status))

        logging.info("Total records: {}".format(self.db.count()))

    def populate_md5s(self):
        logging.info("Computing MD5 sums")  #TODO:  Need to re-query server on cursor timeout.  Trap:  exception pymongo.errors.CursorNotFound(error, code=None, details=None)
#Raised while iterating query results if the cursor is invalidated on the server.
        cursor = self.db.find({'md5': {'$exists': False}})
        total = cursor.count()
        logging.info("MD5 sums needed: {}".format(total))
        for count, record in enumerate(cursor):
            md5sum = file_md5sum(record['path'])
            self.db.update_one({'path': record['path']},{'$set': {'md5': md5sum}})
            if not count % 100:
                logging.info("MD5 sums:  {} of {}, {}%".format(count, total, count/total*100))

    def yield_missing(self, gphotos_db):
        for count, archive_record in enumerate(self.db.find({"g_id": {'$exists': False}})):
            g_record = gphotos_db.find_one({'md5Checksum': archive_record['md5']})
            if not count % 500:
                print("Checked {}".format(count))
            if g_record:
                self.db.update_one({'md5': archive_record['md5']},{'$set': {'g_id': g_record['id']}})
            else:
                yield archive_record['path']

    def check_member(self, md5): # TODO Not done
        return self.db.find({'md5Checksum': md5})


# def move_to_local_archive(archive, photo):
#     if 'md5Checksum' in photo:
#         if archive.check_member(photo['md5Checksum']):
#             safe_move(photo['path'], TEMPORARY_HOLD_DIR)
#         else:
#             safe_move(photo['path'], place_figured_out_from_path_elements)


    #if md5sum already in local archive:
    #   safe move to 'to be deleted'
    #   return
    #build path from parent record
    #safe_move to local archive
    #add checksum to database

class Gphotos(object):
    """
    Gphotos:  A set of tools to aid management of local images and a Google Photos repository
    """
    def __init__(self, database, collection):
        self.service = None
        self.db = pymongo.MongoClient()[database][collection]
        self.db.create_index('id')
        self.db.create_index('md5Checksum')


    def check_member(self, md5):
        """
        If md5 is in Google Photos returns associated Gphoto metadata, otherwise returns None
        :param md5: MD5 sum of record possibly on Google Photos
        :return: dict of matching Google Photo metadata, returns None if not in Google Photos
        """
        meta = self.db.find_one({'md5Checksum': md5, 'trashed': False, 'explicitlyTrashed': False})
        if meta is not None:
            gphoto_path = os.path.join(*(self.db.find_one({'id': meta['parents'][0]})['path']))
            meta.update({'gpath': gphoto_path})
        return meta

    def _walk_error(self, walk_err):
        # TODO: Maybe some better error trapping here...
        print("Error {}:{}".format(walk_err.errno, walk_err.strerror))
        raise

    def check_tree(self, top):
        """
        Descends filesystem from top and returns a dict containing file path for each file.  If a record appears in Google Photos
        with the same MD5 sum then also populates dict with record metadata
        :param top: Root of tree to descend in filesystem
        :return: dict with path of each file, augmented with Google Photo record metadata if record with same MD5 sum appears in Google Photos
        """
        logging.info("Traversing filesystem tree starting at {}...".format(top))
        if os.path.isfile(top):
            yield self.add_filepath_and_lookup(top)
        else:
            for dirpath, dirnames, filenames in os.walk(top, onerror=self._walk_error):
                logging.info('Processing dir {}'.format(dirpath))
                for filepath in [os.path.join(dirpath, filename) for filename in filenames]:
                    yield self.add_filepath_and_lookup(filepath)
        logging.info("Done traversing filesystem tree.")

    def add_filepath_and_lookup(self, filepath):
        member = {'filepath': filepath}
        metadata = self.check_member(file_md5sum(filepath))
        if metadata is not None:
            member.update(self.check_member(file_md5sum(filepath)))
        return member

    def sync(self):
        """
        Synchronize database with google photos
        """

        if self.service is None:
            self.get_service()

        # TODO:  Make sure we don't 'find' files that are marked at trashed
        database_changed = False

        INIT_FIELDS = "files(id,imageMediaMetadata/time,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,spaces,explicitlyTrashed,trashed), nextPageToken"
        change_token_cursor = self.db.find({'change_token': {'$exists': True}})
        assert change_token_cursor.count() <= 1
        if change_token_cursor.count() == 0:  # If we have no change token, drop and resync the database
            logging.info("No change token available - resyncing database")
            self.db.drop()
            database_changed = True
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
            logging.info("Total records: {}".format(self.db.count()))
        else:
            new_count = 0
            delete_count = 0
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
                    database_changed = True
                    for change in changes['changes']:
                        if change['removed'] is True:
                            db_status = self.db.delete_one({'id': change['fileId']})
                            assert db_status.deleted_count == 1, "Deleted files count should be 1, got {}".format(
                                db_status.deleted_count)
                            delete_count += 1
                        else:
                            db_status = self.db.replace_one({'id': change['file']['id']}, change['file'],
                                                       upsert=True)  # TODO:  Make sure the data that comes with change is complete for insertion
      #                      assert db_status.modified_count == 1, "Modified files count should be 1, got {}".format(
      #                          db_status.modified_count)
                            new_count += 1
                if 'nextPageToken' in changes:
                    change_token = changes['nextPageToken']
                else:
                    assert 'newStartPageToken' in changes, "newStartPageToken missing when nextPageToken is missing.  Should never happen."
                    db_status = self.db.replace_one({'change_token': {'$exists': True}},
                                               {'change_token': changes['newStartPageToken']})
                    assert db_status.modified_count == 1, "Database did not update correctly"
                    break  # All changes have been received
            logging.info("Sync update complete.  New files: {} Deleted files: {}".format(new_count, delete_count))
        logging.info('Done with database resync')

        if database_changed:
            self.__get_parents()
            root_id = self.service.files().list(q='name="Google Photos"').execute()['files'][0]['id']
            self.__set_paths(root_id, ['Google Photos'])
            logging.info('Done set_paths')

        def get_stats():
            count = self.db.count()
            # self.db.sales.aggregate([
            #     {
            #         '$group': {_id: {day: {$dayOfYear: "$date"}, year: { $year: "$date"}},
            #                                 totalAmount: { $sum: { $multiply: ["$price", "$quantity"]}},
            #                     count: { $sum: 1}}}
            # ])


    def __get_parents(self):
        """
        Populate database entries for parent folders
        :return: None.  Changes database
        """
        # TODO:  This delivers a datbase record with "My Drive" in it.  That is too high in the tree.....

        if self.service is None:
            self.get_service()

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
        self.service = discovery.build('drive', 'v3', http=http)


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

def file_md5sum(path):
    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!

    md5 = hashlib.md5()
    #
    #
    try:
        f = open(path, 'rb')
    except IOError:
        logging.error("Can't open path {}".format(path))
    else:
        with f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                md5.update(data)
    return md5.hexdigest()

# TODO:  Consider this error handling should files fail to open:
# try:
#     file = open(...)
# except OpenErrors...:
#     # handle open exceptions
# else:
#     try:
#         # do stuff with file
#     finally:
#         file.close()

if __name__ == '__main__':
    main()
