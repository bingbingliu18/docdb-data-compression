#from gevent import monkey; monkey.patch_all()
from multiprocessing import  Process
import json
import pymongo
import time
import datetime
import bson
from pymongo import MongoClient, ASCENDING, UpdateOne, InsertOne, DeleteOne, ReplaceOne

PROCESS_NUM = 20
LOOP_NUM_PER_PROCESS = 10001
with open('/home/ec2-user/data_compression/ck.json') as f:
    data = json.load(f)
def fun1(name):
    client = pymongo.MongoClient("mongodb://masteruser:*****@docdb-dat*****on.cluster-ckutjdcz3cie.us-east-1.docdb.amazonaws.com:27017/?tls=false&retryWrites=false")
    testdb = client["test"]
    ck=testdb['ck_compression_prod']
    for i in range(1, LOOP_NUM_PER_PROCESS):
        sequence = str(name) + str(i)
        ck.insert_one({"num":sequence,"auto_data":data})
           
        if i%1000==0:
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
