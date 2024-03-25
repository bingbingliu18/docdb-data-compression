#from gevent import monkey; monkey.patch_all()
from multiprocessing import  Process
import json
import pymongo
import time
import datetime
import bson
from pymongo import MongoClient, ASCENDING, UpdateOne, InsertOne, DeleteOne, ReplaceOne
from pymongo.write_concern import WriteConcern
#from pymongo.MongoClient import write_concern
PROCESS_NUM = 300
LOOP_NUM_PER_PROCESS = 1001
with open('/root/game_test/player_data.json') as f:
    data = json.load(f)
def fun1(name):
    client = pymongo.MongoClient("mongodb://root:******@dds-bp15cb9938c2a8f42.mongodb.rds.aliyuncs.com:3717,dds-bp15cb9938c2a8f41.mongodb.rds.aliyuncs.com:3717/?replicaSet=mgset-76203820&w=1")
    #client.write_concern = {'w': 1, 'wtimeout': 1000, 'j': True}
    testdb = client["test"]
    #testdb = client.get_database(
    #            "test", write_concern=WriteConcern(w=0))
    #player=testdb['player'].with_options(write_concern={w':1})
    #new_player=testdb['player'].with_options(write_concern=WriteConcern(w=0))
    player=testdb['player']
    for i in range(1, LOOP_NUM_PER_PROCESS):
        sequence = str(name) + str(i)
        start_time = time.time()
        player.insert_one({"num":sequence,"profile_data":data})
        #player.with_options(write_concern=WriteConcern(wtimeout:5000, w:0)).insert_one({"num":sequence,"profile_data":data})
        end_time = time.time()
        print(f"execute time: {end_time - start_time} seconds")


        if i%100==0:
            save_time = time.strftime('%m-%d %H:%M:%S', time.localtime())
            print ("process "+str(name)+" "+save_time+" have finished "+str(i)+" data insert")
            # print "process "+str(name)+" "+save_time+" have finished "+str(i)+" data insert"
if __name__ == '__main__':
    process_list = []
    start_time = time.time()
    loop_start_time = time.strftime('%m-%d %H:%M:%S', time.localtime())
    print ("loop_start_time", loop_start_time)
    #print "loop_start_time", loop_start_time
    for i in range(PROCESS_NUM):
        p = Process(target=fun1,args=(str(i),))
        p.start()
        process_list.append(p)

    for i in process_list:
        p.join()

    loop_end_time = time.strftime('%m-%d %H:%M:%S', time.localtime())
    print ("loop_end_time", loop_end_time)
    #print "loop_end_time", loop_end_time
    end_time = time.time()
    print ("*************************************TEST END***********************************")
    print ("Insert data number: ", (LOOP_NUM_PER_PROCESS -1) * PROCESS_NUM)
    print ("Time elapse seconds:", end_time - start_time)
    print ("Insert speed", (LOOP_NUM_PER_PROCESS  - 1) * PROCESS_NUM / (end_time - start_time))
