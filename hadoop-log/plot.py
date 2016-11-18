#!/usr/bin/python

import sys, getopt
import matplotlib.pyplot as plt
import numpy as np


jobIdToMap = {}
jobIdToReduce = {}
taskIdToData = {}

mapPropertiesNum = 8
reducePropertiesNum = 8


def parse_file(inputfile):
    f = open(inputfile, 'r')
    for line in f:
        if 'frankfzw' not in line:
            continue

        target = line[line.index('frankfzw'):]

        # is it a init log?
        if 'isMap' in target:
            array = target.split()
            ids = array[3].split(':')
            taskIdToData[ids[1]] = {}
            # record init time of a task
            taskIdToData[ids[1]]['init'] = array[6]
            if array[2] == 'true':
                if ids[0] in jobIdToMap:
                    jobIdToMap[ids[0]].append(ids[1])
                else:
                    jobIdToMap[ids[0]] = [ids[1]]
                taskIdToData[ids[1]]['map starts ts'] = 0
                taskIdToData[ids[1]]['shuffle start ts'] = 0
                taskIdToData[ids[1]]['shuffle write'] = 0
                taskIdToData[ids[1]]['shuffle size'] = 0
                taskIdToData[ids[1]]['map compute start ts'] = 0
                taskIdToData[ids[1]]['map compute interval'] = 0
                taskIdToData[ids[1]]['map interval'] = 0

            else:
                if ids[0] in jobIdToReduce:
                    jobIdToReduce[ids[0]].append(ids[1])
                else:
                    jobIdToReduce[ids[0]] = [ids[1]]
                taskIdToData[ids[1]]['reduce start ts'] = 0
                taskIdToData[ids[1]]['shuffle start ts'] = 0
                taskIdToData[ids[1]]['fetch'] = 0
                taskIdToData[ids[1]]['reduce compute start ts'] = 0
                taskIdToData[ids[1]]['reduce compute interval'] = 0
                taskIdToData[ids[1]]['reduce interval'] = 0


        # deal with reduce task
        elif 'ReduceTask' in line:
            array = target.split()
            ids = array[1].split(':')
            if 'reduce starts' in target:
                taskIdToData[ids[1]]['reduce start ts'] = array[5]
            elif 'shuffle starts' in target:
                taskIdToData[ids[1]]['shuffle start ts'] = array[5]
            elif 'shuffle fetch' in target:
                taskIdToData[ids[1]]['fetch'] = array[6]
            elif 'reduce compute starts' in target:
                taskIdToData[ids[1]]['reduce compute start ts'] = array[6]
            elif 'reduce compute finished' in target:
                taskIdToData[ids[1]]['reduce compute interval'] = array[6]
            else:
                taskIdToData[ids[1]]['reduce interval'] = array[5]
        
        #deal with map task
        elif 'MapTask' in line:
            array = target.split()
            ids = array[1].split(':')
            if 'map starts' in target:
                taskIdToData[ids[1]]['map start ts'] = array[5]
            elif 'flush' in target:
                taskIdToData[ids[1]]['shuffle start ts'] = array[8]
            elif 'shuffle write' in target:
                taskIdToData[ids[1]]['shuffle write'] = array[6]
                taskIdToData[ids[1]]['shuffle size'] = array[10]
            elif 'map compute starts' in target:
                taskIdToData[ids[1]]['map compute start ts'] = array[6]
            elif 'map compute finished' in target:
                taskIdToData[ids[1]]['map compute interval'] = array[6]
            elif 'map finished' in target:
                taskIdToData[ids[1]]['map interval'] = array[5]


def plot():
    plt.rcdefaults()
    for jobId in jobIdToMap:
        mapTaskId = jobIdToMap[jobId]
        reduceTaskId = jobIdToReduce[jobId]
        y_pos = np.arange(len(mapTaskId) + len(reduceTaskId))
        x_pos = np.zeros(len(y_pos))
        
        #print init time
        width = np.asarray(map(lambda x:int(taskIdToData[x]['init']), mapTaskId))
        width = np.append(width, np.zeros(len(reduceTaskId)))
        plt.barh(y_pos, width, height=0.5, left=x_pos, color='blue')

        #print map compute time
        x_pos = np.asarray(map(lambda x,y:x+y, x_pos, width))
        width = np.asarray(map(lambda x:int(taskIdToData[x]['map compute interval']), mapTaskId))
        width = np.append(width, np.zeros(len(reduceTaskId)))
        plt.barh(y_pos, width, height=0.5, left=x_pos, color='green')

        #print shuffle wait
        x_pos = np.asarray(map(lambda x,y:x+y, x_pos, width))
        width = np.asarray(map(lambda x:
            (0 if (taskIdToData[x]['shuffle start ts'] == 0) else (int(taskIdToData[x]['shuffle start ts'])-int(taskIdToData[x]['map start ts'])-int(taskIdToData[x]['map compute interval']))), mapTaskId))
        width = np.append(width, np.zeros(len(reduceTaskId)))
        plt.barh(y_pos, width, height=0.5, left=x_pos, color='black')

        #print shuffle write
        x_pos = np.asarray(map(lambda x,y:x+y, x_pos, width))
        width = np.asarray(map(lambda x:int(taskIdToData[x]['shuffle write']), mapTaskId))
        width = np.append(width, np.zeros(len(reduceTaskId)))
        plt.barh(y_pos, width, height=0.5, left=x_pos, color='red')

        #print reduce 
        #make distance from reduce phase and map phase
        x_pos = np.asarray(map(lambda x,y:x+y, x_pos, width))
        maxMapTs = np.amax(x_pos)
        x_pos = np.zeros(len(y_pos))
        x_pos.fill(2000 + maxMapTs)
        
        #print reduce init time
        width = np.asarray(map(lambda x:int(taskIdToData[x]['init']), reduceTaskId))
        width = np.append(np.zeros(len(mapTaskId)), width)
        plt.barh(y_pos, width, height=0.5, left=x_pos, color='blue')

        #print shuffle read
        x_pos = np.asarray(map(lambda x,y:x+y, x_pos, width))
        width = np.asarray(map(lambda x:int(taskIdToData[x]['fetch']), reduceTaskId))
        width = np.append(np.zeros(len(mapTaskId)), width)
        plt.barh(y_pos, width, height=0.5, left=x_pos, color='yellow')

        #print reduce compute
        x_pos = np.asarray(map(lambda x,y:x+y, x_pos, width))
        width = np.asarray(map(lambda x:int(taskIdToData[x]['reduce compute interval']), reduceTaskId))
        width = np.append(np.zeros(len(mapTaskId)), width)
        plt.barh(y_pos, width, height=0.5, left=x_pos, color='green')


        mapTaskId.extend(reduceTaskId)
        plt.yticks(y_pos, np.asarray(mapTaskId))
        print y_pos
        plt.show()



def main(argv):
    inputfile = ''
    # get input file path from arguements
    try:
        opts, args = getopt.getopt(argv, "hi:")
    except getopt.GetoptError:
        print 'plot.py -i <inputfile>'
        sys.exit(1)

    for opt, arg in opts:
        if opt == '-h':
            print 'plot.py -i <inputfile>'
            sys.exit()
        elif opt == '-i':
            inputfile = arg


    print 'Input file is ' + inputfile

    parse_file(inputfile)
    plot()

    # for k in jobIdToMap:
    #     print jobIdToMap[k]
    #     
    # for k in jobIdToReduce:
    #     print jobIdToReduce[k]
    # 
    # for k in taskIdToData:
    #     print taskIdToData[k]


if __name__ == "__main__":
    main(sys.argv[1:])
