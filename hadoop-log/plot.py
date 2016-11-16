#!/usr/bin/python

import sys, getopt


jobIdToMap = {}
jobIdToReduce = {}
taskIdToData = {}


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
            else:
                if ids[0] in jobIdToReduce:
                    jobIdToReduce[ids[0]].append(ids[1])
                else:
                    jobIdToReduce[ids[0]] = [ids[1]]

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

    for k in jobIdToMap:
        print jobIdToMap[k]
        
    for k in jobIdToReduce:
        print jobIdToReduce[k]
    
    for k in taskIdToData:
        print taskIdToData[k]


if __name__ == "__main__":
    main(sys.argv[1:])
