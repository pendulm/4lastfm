import csv
import json
import cPickle
from utils import is_recent, strptime
field_name = ['id', 'name', 'url', 'artist', 'duration', 'listeners', 'playcount',
      'streamable', 'total_shouts', 'releasedate']

f = open("../data/top_1000_tracks.pkl", "rb")
tracks = cPickle.load(f)['tracks']

with open('1000_tracks_info.csv', 'wb') as csvfile:
    w = csv.DictWriter(csvfile, field_name, extrasaction='ignore')
    first = dict((f, f) for f in field_name)
    w.writerow(first)
    num = 1
    for t in tracks:
        t['artist'] = t['artist']['name']
        releasedate = t['releasedate'].strip()
        for i in t:
            value = t[i]
            if isinstance(value, unicode):
                t[i] = value.encode('utf-8')
        w.writerow(t)
        print num
        num += 1

