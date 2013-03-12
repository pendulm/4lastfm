import csv
import json
import cPickle
field_name = ['name', 'realname', 'url', 'country', 'age', 'gender',
      'bootstrap', 'playlists', 'registered', 'subscriber',
      'playcount', 'type', 'id']

fi = open("../data/target_users.pkl", "r")
users = cPickle.load(fi)
male = 0
female = 0
unknown = 0

with open('../results/user_info.csv', 'wb') as csvfile:
    w = csv.DictWriter(csvfile, field_name, extrasaction='ignore')
    first = dict((f, f) for f in field_name)
    w.writerow(first)
    for (n, u) in users.iteritems():
        for i in u:
            value = u[i]
            if isinstance(value, unicode):
                u[i] = value.encode('utf-8')
        if u['gender'] == 'm':
            male += 1
        elif u['gender'] == 'f':
            female += 1
        else:
            unknown += 1
        w.writerow(u)
print "male=%d female=%d unknown=%d" % (male, female, unknown)
