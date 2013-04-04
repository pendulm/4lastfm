import csv
import json
import cPickle
from utils import is_recent, strptime
field_name = ['id', 'name', 'url', 'artist', 'duration', 'listeners', 'playcount',
      'streamable', 'total_shouts', 'releasedate']

f = open("../data/recent_tracks.pkl", "rb")
tracks = cPickle.load(f)

with open('../results/tracks_info.csv', 'wb') as csvfile:
    w = csv.DictWriter(csvfile, field_name, extrasaction='ignore')
    first = dict((f, f) for f in field_name)
    w.writerow(first)
    for t in tracks:
        t['artist'] = t['artist']['name']
        releasedate = t['releasedate'].strip()
        if not releasedate or not is_recent(strptime(releasedate)):
            continue
        for i in t:
            value = t[i]
            if isinstance(value, unicode):
                t[i] = value.encode('utf-8')
        w.writerow(t)

