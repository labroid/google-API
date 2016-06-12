import pymongo
import arrow

db = pymongo.MongoClient().gp.gp_batch
start = arrow.now().float_timestamp
print("starting index....")
db.create_index('id')
print("Time to index:{}".format(arrow.now().float_timestamp - start))

def get_node_meta(db, id):
    node_meta = db.find_one({'id': id})
    if not node_meta:
        #go to google to get node_meta
        pass
#    print(node_meta['title'])
    return node_meta

def find_path(db, id):
    paths = []
    node_meta = get_node_meta(db, id)
    for parent_meta in node_meta['parents']:  #If no parents, then returns empty list, which is ok since you are at root
        parent_node_meta = get_node_meta(db, parent_meta['id'])
        if parent_node_meta is None:
            paths.append('My Drive') #TODO: Might not need once google search is implemented
        elif parent_meta['isRoot']:  #parent is root
            paths.append(parent_node_meta['title'])
        else:
            new_paths = find_path(db, parent_meta['id'])
            for new_path in new_paths:
                paths.append(new_path + '/' + parent_node_meta['title'])
#    print(paths)
    return paths


def pathfinder():
    meta_cursor = db.find({'id': '0B8p6GgTlTvdrQ3h2bVhLUXpQb0U'})
    for count, meta in enumerate(meta_cursor):
        paths = find_path(db, meta['id'])
        if len(paths) > 1:
            print(count, meta['title'], paths)
        if not count % 1000:
            print(count)

def main():
    pathfinder()

if __name__ == '__main__':
    main()