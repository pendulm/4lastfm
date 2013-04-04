import csv
import json
import cPickle
field_name = ['name', 'url', 'mbid', 'first_release', 'last_realease']

fi = open("../data/artists_info.pkl", "r")
art_info = cPickle.load(fi)

with open('../results/artists_info.csv', 'wb') as csvfile:
    w = csv.DictWriter(csvfile, field_name, extrasaction='ignore')
    first = dict((f, f) for f in field_name)
    w.writerow(first)
    for (_, u) in art_info.iteritems():
        for i in u:
            value = u[i]
            if isinstance(value, unicode):
                u[i] = value.encode('utf-8')
        u['first_release'] = None
        u['last_realease'] = None
        w.writerow(u)
