import os
from pprint import pprint
import sys
import os.path
import json

import time
import datetime


def parse_disk_util(filename, group_by):
    disk_utils=[]
    with open(filename) as f:
        content = f.readlines()
        for line in content:
            if "avg" in line:
                continue
            if "disk" not in line:
                continue
            tputs = line.split(",")[1:-1]
            # print tputs
            ctput=0
            disk_util=[]
            for i in range(len(tputs)):
                if i != 0 and i % group_by == 0:
                    disk_util.append(ctput / group_by)
                    # print "{},{}".format(i / group_by, ctput / group_by)
                    ctput = 0
                ctput += float(tputs[i])
            disk_utils.append(disk_util)
    header=""
    for i in range(len(disk_utils)):
        header += "disk-{}".format(i)
        header += ","

    print header
    for time in range(len(disk_utils[0])):
        thpt="{},".format(time)
        for i in range(len(disk_utils)):
            thpt += str(disk_utils[i][time])
            thpt += ","
        print thpt

    for i in range(len(disk_utils)):
        thpt=0
        times=0
        for time in range(len(disk_utils[i])):
            t = disk_utils[i][time]
            if t > 5:
                thpt += t
                times += 1
        print "disk-{},{}".format(i, thpt / times)

def parse_tput(filename, group_by):
    with open(filename) as f:
        content = f.readlines()
        tputs = content[0].split(",")
        ctput=0
        for i in range(len(tputs)):
          if i != 0 and i % group_by == 0:
              print "{},{}".format(i / group_by, ctput / group_by)
              ctput = 0
          ctput += float(tputs[i])

def parse_migration_stoc(filename):
    with open(filename) as f:
        content = f.readlines()
        s=0
        for line in content:
            if "Migrate StoC" in line:
                print s
                s=0
                continue
            val=float(line.split(",")[-3])+float(line.split(",")[-4])
            s+= val

filename=sys.argv[1]
group_by=int(sys.argv[2])
parse_disk_util(filename, group_by)
# parse_tput(filename, group_by)

# parse_migration_stoc(filename)

# parse_disk_util(filename, group_by)

