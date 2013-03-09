import csv
import json
import cPickle
from datetime import datetime
field_name = ['id', 'name', 'url', 'artist', 'duration', 'listeners', 'playcount',
      'streamable', 'total_shouts', 'releasedate']

f = open("../data/for_track.pkl", "rb")
tracks = cPickle.load(f)['tracks']

def strptime(date_string):
    date_string = str(date_string.strip())
    time_format = "%d %b %Y, %H:%M"
    return datetime.strptime(date_string, time_format)

def get_n_months_ago(dt, n):
    year = dt.year
    month = dt.month
    day = dt.day
    if month - n <= 0:
        year -= 1
    month = (month - n) % 12
    if month == 0:
        month = 12
    return datetime(year, month, day)

def is_recent(dt):
    now = datetime.now()
    six_months_ago = get_n_months_ago(now, 6) 
    if dt > six_months_ago:
        return True
    return False

with open('tracks_info.csv', 'wb') as csvfile:
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

