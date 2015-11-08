

import os
import os.path
import hashlib
import pymongo

def file_present(path, name):
    print "File {} found as {}".format(path, name)

def file_missing(path, md5sum):
    print "File {} missing {}".format(path, md5sum)

def walkerror():
    print "Whoa - error"

def main():
    count = 0
    present = 0
    missing = 0
    top = r"C:\Users\scott_jackson\Pictures"
    db = pymongo.MongoClient().gp.gp_collection
    for dirpath, dirnames, filenames in os.walk(top, topdown=TabError, onerror=walkerror):
        for filename in filenames:
            count += 1
            filepath = os.path.join(dirpath, filename)
#            md5sum = hashlib.md5(open(filepath, 'rb').read()).hexdigest()
#            record = db.find_one({'md5Checksum': md5sum})
            record = db.find_one({'title': filename})
            if record:
                #file_present(filepath, record['title'])
                present += 1
            else:
#                file_missing(filepath, md5sum)
               # file_missing(filepath, filename)
                missing += 1

            if not count % 100:
                print count, present, missing
    print "done"
if __name__ == '__main__':
    main()
