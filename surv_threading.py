import RPi.GPIO as GPIO
import subprocess
import time
import sys
import os
import random
import string
import threading
import boto3
import Queue


sensor = 12
PATH_CLOUD = "/home/pi/CloudComputingProj1"
PATH_FACEDETECT = "/home/pi/facedetect"
PATH_DARKNET="/home/pi/darknet"
GPIO.setwarnings(False)
GPIO.setmode(GPIO.BOARD)
GPIO.setup(sensor, GPIO.IN)
ec2_client = boto3.client('ec2',region_name="us-east-1")


MAX_NUM_THREADS = 1
MAX_QUEUE_SIZE = int(sys.argv[1])
NUM_VIDEOS_NEEDED = int(sys.argv[2])

q = Queue.Queue(MAX_QUEUE_SIZE)


def thread_function(filename):
    #os.chdir(PATH_DARKNET)
    command  = "./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights /home/pi/facedetect/" + filename + " > results.txt"
    print command
    process = subprocess.Popen(command,shell=True, cwd=PATH_DARKNET)
    process.wait()
    key = filename.split(".")[0]
    command2 = "python processPiResults.py " + key
    process2 = subprocess.Popen(command2, shell=True, cwd=PATH_CLOUD)
    process2.wait()
    #os.chdir(PATH_FACEDETECT)
    print "thread exited (Should have)"

class ConsumerThread(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(ConsumerThread,self).__init__()
        self.target = target
        self.name = name
        return

    def run(self):
        while True:
            if not q.empty():

                filename = q.get()
                print "Filename: " + filename + " being processed by " + self.name
                thread_function(filename)
                q.task_done()
            time.sleep(3)
        return



## Starting All the worker threads

print "STARTING ALL WORKER THREADS\n\n"

for i in range(MAX_NUM_THREADS):
    c = ConsumerThread(name="worker_"+str(i+1))
    c.start()





def generate_random_object_name(stringLength = 10):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

on = 0
off = 0
flag = 0
t=None

while NUM_VIDEOS_NEEDED:
    i=GPIO.input(sensor)
    if i == 0:
        off = time.time()
        diff = off - on
        print 'time: ' + str(diff%60) + ' sec'
        print ''
        print "No intruders"
        time.sleep(1)
    elif i == 1:
        print "Intruder detected"
        on = time.time()
        key = generate_random_object_name(5)
        filename = key + '.h264'
        
        process1 = subprocess.Popen('python /home/pi/facedetect/take_snapshot.py ' + filename, shell=True)
        process1.wait()
        NUM_VIDEOS_NEEDED-=1
        
        if q.full():
            print("\n \nQueue is full, Sending to Cloud to process \n\n")
            process2 = subprocess.Popen( 'python uploadFile.py ' + PATH_FACEDETECT+'/'+filename, shell=True, cwd=PATH_CLOUD)
            process2.wait()

        else:
            print("\n\nQueue not full, Adding filename to Queue\n\n")
            q.put(filename)
	
	# if os.path.exists(filename):
	# 	os.remove(filename)
 #        time.sleep(0.1)
