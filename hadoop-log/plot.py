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
            taskIdToData[ids[1]]['init'] = int(array[6])
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
                taskIdToData[ids[1]]['reduce start ts'] = int(array[5])
            elif 'shuffle starts' in target:
                taskIdToData[ids[1]]['shuffle start ts'] = int(array[5])
            elif 'shuffle fetch' in target:
                taskIdToData[ids[1]]['fetch'] = int(array[6])
            elif 'reduce compute starts' in target:
                taskIdToData[ids[1]]['reduce compute start ts'] = int(array[6])
            elif 'reduce compute finished' in target:
                taskIdToData[ids[1]]['reduce compute interval'] = int(array[6])
            else:
                taskIdToData[ids[1]]['reduce interval'] = int(array[5])
        
        #deal with map task
        elif 'MapTask' in line:
            array = target.split()
            ids = array[1].split(':')
            if 'map starts' in target:
                taskIdToData[ids[1]]['map start ts'] = int(array[5])
            elif 'flush' in target:
                taskIdToData[ids[1]]['shuffle start ts'] = int(array[8])
            elif 'shuffle write' in target:
                taskIdToData[ids[1]]['shuffle write'] = int(array[6])
                taskIdToData[ids[1]]['shuffle size'] = int(array[10])
            elif 'map compute starts' in target:
                taskIdToData[ids[1]]['map compute start ts'] = int(array[6])
            elif 'map compute finished' in target:
                taskIdToData[ids[1]]['map compute interval'] = int(array[6])
            elif 'map finished' in target:
                taskIdToData[ids[1]]['map interval'] = int(array[5])


def sort_ts(taskId, start, end, task_property):
    if start >= end:
        return

    original_start = start
    original_end = end
    flag = taskIdToData[taskId[start]][task_property]
    start = start + 1
    while start < end:
        while taskIdToData[taskId[start]][task_property] < flag:
            start = start + 1
            if (start >= end):
                break
        while taskIdToData[taskId[end]][task_property] >= flag:
            end = end - 1
            if (end <= start):
                break
        if start < end:
            taskId[start], taskId[end] = taskId[end], taskId[start]
            start = start + 1
            end = end - 1
    if taskIdToData[taskId[original_start]][task_property] > taskIdToData[taskId[start]][task_property]:
        taskId[original_start], taskId[start] = taskId[start], taskId[original_start] 

    sort_ts(taskId, original_start, start - 1, task_property)
    sort_ts(taskId, start, original_end, task_property)


def plot():
    plt.rcdefaults()
    for jobId in jobIdToMap:
        mapTaskId = jobIdToMap[jobId]
        sort_ts(mapTaskId, 0, len(mapTaskId)-1, 'map start ts')
        reduceTaskId = jobIdToReduce[jobId]
        sort_ts(reduceTaskId, 0, len(reduceTaskId)-1, 'reduce start ts')
        # y_pos = np.arange(len(mapTaskId) + len(reduceTaskId))
        # x_pos = np.zeros(len(y_pos))
        map_start_ts = np.asarray(map(lambda x:taskIdToData[x]['map start ts'], mapTaskId))
        min_map_start_ts = np.amin(map_start_ts)

        y_pos = np.arange(len(mapTaskId))
        x_pos = np.asarray(map(lambda x:x-min_map_start_ts, map_start_ts))
        
        #print init time
        width = np.asarray(map(lambda x:taskIdToData[x]['init'], mapTaskId))
        # width = np.append(width, np.zeros(len(reduceTaskId)))
        m_init = plt.barh(y_pos, width, height=0.5, left=x_pos, color='none', hatch='////', align='center')
        # xmin = x_pos
        # xmax = np.asarray(map(lambda x, y:x+y, x_pos, width))
        # plt.hlines(y_pos, xmin, xmax, linestyle='solid', linewidths='3')

        #print map compute time
        x_pos = np.asarray(map(lambda x,y:x+y, x_pos, width))
        width = np.asarray(map(lambda x:taskIdToData[x]['map compute interval'], mapTaskId))
        # width = np.append(width, np.zeros(len(reduceTaskId)))
        m_compute = plt.barh(y_pos, width, height=0.5, left=x_pos, color='none', hatch='....', align='center')
        # xmin = x_pos
        # xmax = np.asarray(map(lambda x, y:x+y, x_pos, width))
        # plt.hlines(y_pos, xmin, xmax, linestyle=':', linewidths='3')


        #print shuffle wait
        x_pos = np.asarray(map(lambda x,y:x+y, x_pos, width))
        width = np.asarray(map(lambda x:
            (0 if (taskIdToData[x]['shuffle start ts'] == 0) else (taskIdToData[x]['shuffle start ts']-taskIdToData[x]['map start ts']-taskIdToData[x]['map compute interval'])), mapTaskId))
        # width = np.append(width, np.zeros(len(reduceTaskId)))
        m_shuffle_wait = plt.barh(y_pos, width, height=0.5, left=x_pos, color='none', hatch='----', align='center')
        # xmin = x_pos
        # xmax = np.asarray(map(lambda x, y:x+y, x_pos, width))
        # plt.hlines(y_pos, xmin, xmax, linestyle='--', linewidths='3')


        #print shuffle write
        x_pos = np.asarray(map(lambda x,y:x+y, x_pos, width))
        width = np.asarray(map(lambda x:taskIdToData[x]['shuffle write'], mapTaskId))
        # width = np.append(width, np.zeros(len(reduceTaskId)))
        m_shuffle_write = plt.barh(y_pos, width, height=0.5, left=x_pos, color='none', hatch='++++', align='center')
        # xmin = x_pos
        # xmax = np.asarray(map(lambda x, y:x+y, x_pos, width))
        # plt.hlines(y_pos, xmin, xmax, linestyle='-.', linewidths='3')


        #print reduce 
        #make distance from reduce phase and map phase
        y_pos = np.arange(len(mapTaskId)+1, len(mapTaskId)+len(reduceTaskId)+1)
        x_pos = np.asarray(map(lambda x,y:x+y, x_pos, width))
        maxMapTs = np.amax(x_pos)

        x_pos = np.asarray(map(lambda x:taskIdToData[x]['reduce start ts']-min_map_start_ts, reduceTaskId))
        
        #print vline to seperate map and reduce
        plt.plot([10 + maxMapTs, 10 + maxMapTs], [-0.5, len(mapTaskId) + len(reduceTaskId)], 'k--')
        
        #print reduce init time
        width = np.asarray(map(lambda x:taskIdToData[x]['init'], reduceTaskId))
        # width = np.append(np.zeros(len(mapTaskId)), width)
        r_init = plt.barh(y_pos, width, height=0.5, left=x_pos, color='none', hatch='////', align='center')
        # xmin = x_pos
        # xmax = np.asarray(map(lambda x, y:x+y, x_pos, width))
        # plt.hlines(y_pos, xmin, xmax, linestyle='solid', linewidths='3')


        #print shuffle read
        x_pos = np.asarray(map(lambda x,y:x+y, x_pos, width))
        width = np.asarray(map(lambda x:taskIdToData[x]['fetch'], reduceTaskId))
        # width = np.append(np.zeros(len(mapTaskId)), width)
        r_shuffle_read = plt.barh(y_pos, width, height=0.5, left=x_pos, color='none', hatch='xxxx', align='center')
        # xmin = x_pos
        # xmax = np.asarray(map(lambda x, y:x+y, x_pos, width))
        # plt.hlines(y_pos, xmin, xmax, linestyle='-.', linewidths='3')


        #print reduce compute
        x_pos = np.asarray(map(lambda x,y:x+y, x_pos, width))
        width = np.asarray(map(lambda x:taskIdToData[x]['reduce compute interval'], reduceTaskId))
        # width = np.append(np.zeros(len(mapTaskId)), width)
        r_compute = plt.barh(y_pos, width, height=0.5, left=x_pos, color='none', hatch='....', align='center')
        # xmin = x_pos
        # xmax = np.asarray(map(lambda x, y:x+y, x_pos, width))
        # plt.hlines(y_pos, xmin, xmax, linestyle=':', linewidths='3')

        #print hline to speprate map and reduce
        xmin, xmax = plt.xlim()
        plt.plot([xmin, xmax], [len(mapTaskId), len(mapTaskId)], 'k--')

        #add legend
        plt.legend((m_init[0], m_compute[0], m_shuffle_wait[0], m_shuffle_write[0], r_shuffle_read[0]), 
                ('map/reduce init', 'map/reduce compute', 'map shuffle wait', 'map shuffle write', 'reduce shuffle read'), loc='lower right')

        map_task_id_len = len(mapTaskId)
        mapTaskId.extend(reduceTaskId)
        labels = map(lambda x:x.split('_', 3)[3], mapTaskId)
        labels.insert(map_task_id_len, '')
        y_pos = np.arange(len(mapTaskId)+len(reduceTaskId)+1)
        plt.yticks(y_pos, np.asarray(labels))
        plt.ylim(-0.5, (len(y_pos)-1.5))
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
