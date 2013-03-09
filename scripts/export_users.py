import csv
import json
field_name = ['name', 'realname', 'url', 'country', 'age', 'gender',
      'bootstrap', 'playlists', 'registered', 'subscriber',
      'playcount', 'type', 'id']

origin_file = open("user_info.txt", "r")

with open('user_info.csv', 'wb') as csvfile:
    w = csv.DictWriter(csvfile, field_name, extrasaction='ignore')
    first = dict((f, f) for f in field_name)
    w.writerow(first)
    for l in origin_file:
        r = json.loads(l)
        for i in r:
            value = r[i]
            if isinstance(value, unicode):
                r[i] = value.encode('utf-8')
        w.writerow(r)

