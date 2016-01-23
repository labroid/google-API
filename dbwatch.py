import pymongo
import time

#Connect to MongoDB
db = pymongo.MongoClient().gp.gp_collection

last_count = db.count()
while True:
    start = time.time()
    time.sleep(5)
    count = db.count()
    delta = count-last_count
    elapsed = time.time() - start
    print("{} files in {:.1f} = {:.1f} f/s = {:.1f} hrs/200k files, {:.2f} remain".format(count, elapsed, delta/elapsed, 200000.0 * elapsed/delta/3600.0, (200000.0 - count) * elapsed/delta/3600.0))
    last_count = count