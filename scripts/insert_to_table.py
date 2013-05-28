# coding:utf-8
import cPickle as pickle
import sqlite3
from datetime import datetime

def strptime(date_string, time_format="%d %b %Y, %H:%M"):
    date_string = str(date_string.strip())
    return datetime.strptime(date_string, time_format)

def normal_date(date_s):
    dt = strptime(date_s)
    return dt.strftime("%Y/%m/%d")

data_dir = "../data/week_b12/"
conn = sqlite3.connect(data_dir + "listen_history.db")
cur = conn.cursor()

cur.executescript("""create table if not exists targets
                    (userID unique,
                     username,
                     age,
                     country,
                     registered,
                     gender,
                     userPlayCount,
                     listenedArtist,
                     FriendNum,
                     userType);
                 """)


cur.executescript("""create table if not exists tracks
                    (trackID unique,
                     trackname,
                     trackPlayCnt,
                     Shouts,
                     listeners,
                     artist,
                     duration,
                     releasedate)
                 """)

cur.executescript("""create table if not exists artists
                    (trackID unique,
                     trackname,
                     trackPlayCnt,
                     Shouts,
                     listeners,
                     artist,
                     duration,
                     releasedate)
                 """)

# first insert tracks
def insert_tracks():
    tracks_info = pickle.load(open(data_dir + "tracks_info.pkl"))
    for t in tracks_info:
        print t['name']
        cur.execute("insert into tracks values (?,?,?,?,?,?,?,?)",
                   (t['id'], t['name'], t['playcount'], t['total_shouts'],
                    t['listeners'], t['artist']['name'], t['duration'],
                    normal_date(t['releasedate'])
                   )
                  )
    conn.commit()

def insert_targets():
    targets_info = pickle.load(open(data_dir + "target_users.pkl"))
    listenedArtist = {}
    target_friends = pickle.load(open(data_dir + "target_friends.pkl"))

    for l in open(data_dir + "user_artist_stat.txt"):
        name, cnt, _ = l.strip().split("|")
        listenedArtist[name] = cnt

    for name, t in targets_info.iteritems():
        print name
        cur.execute("insert into targets values (?,?,?,?,?,?,?,?,?,?)",
                    (t['id'], t['name'], t['age'], t['country'],
                     t['registered']['#text'].split()[0],
                     t['gender'],
                     t['playcount'],
                     listenedArtist[t['name']],
                     len(target_friends[name]),
                     t['type']
                    )
                  )
    conn.commit()

def insert_artists():
    pass


if __name__ == "__main__":
    # insert_tracks()
    insert_targets()
