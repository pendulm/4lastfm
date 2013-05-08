# coding: utf-8
import pickle
from datetime import datetime

def strptime(date_string, time_format="%d %b %Y, %H:%M"):
    date_string = str(date_string.strip())
    return datetime.strptime(date_string, time_format)

def timestamp(date_string):
    dt = strptime(date_string)
    epoch = datetime.fromtimestamp(0)
    seconds = int((dt - epoch).total_seconds())
    return seconds

tracks_info = pickle.load(open("../data/tracks_info.pkl"))
releasetime = {t['name']: timestamp(t['releasedate']) for t in tracks_info}

with open("../data/friends_and_tracks_and_timestamp.txt", "w") as output:
    for l in open("../data/friends_and_tracks.txt"):
        l = l.decode('utf-8').strip()
        user, track = l.split('|')
        print >> output, "%s|%s|%s" % (user.encode('utf-8')
                ,track.encode('utf-8'), releasetime[track])
