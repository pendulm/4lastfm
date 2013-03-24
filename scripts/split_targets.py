import csv
import json
import cPickle
import math

targets_file = "../data/target_users.pkl"
num = 2
targets = cPickle.load(open(targets_file))
per = int(math.ceil(1000.0 / num))

count = 1
current = 1
bags = []
for t in targets:
    bags.append(t)
    if count == 1:
        filename = "../data/targets_%d.pkl" % current
        f = open(filename, "w")
        count += 1
    elif count == per:
        cPickle.dump(bags, f, 2)
        f.flush()
        print "save %d targets to file %s" % (len(bags), filename)
        count = 1
        current += 1
        bags = []
    else:
        count += 1
else:
    if count != 1 and count <= per:
        cPickle.dump(bags, f, 2)
        f.flush()
        print "save %d targets to file %s" % (len(bags), filename)
