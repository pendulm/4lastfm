# coding:utf-8
import cPickle as pickle
import sqlite3
import subprocess
import os.path
import sys
from datetime import datetime


class HistoryTable(object):
    def __init__(self, week):
        self.week = week
        self.week_dir = "../data/week_%s/" % week
        self.conn = sqlite3.connect(self.week_dir + "listen_history.db")
        self.cur = self.conn.cursor()

    def update_targets(self):
        targets_file = self.week_dir + "target_users.pkl"
        listenedArtist = {}
        for l in open(self.week_dir + "user_artist_stat.txt"):
            name, cnt, _ = l.strip().split("|")
            listenedArtist[name] = cnt

        targets = pickle.load(open(targets_file))
        for name, t in targets.iteritems():
            if t is None:
                continue
            self.cur.execute("update targets set age = ?, country = ?, gender = ?, userPlayCount = ?, listenedArtist = ?, userType = ? where userID = ?; ",
                                                 (t['age'],
                                                  t['country'],
                                                  t['gender'],
                                                  t['playcount'],
                                                  listenedArtist[t['name']],
                                                  t['type'],
                                                  t['id'])
            )

        self.conn.commit()


    def update_tracks(self):
        tracks_file = self.week_dir + "tracks.pkl"
        tracks = pickle.load(open(tracks_file))
        for t in tracks:
            self.cur.execute("update tracks set trackPlayCnt = ?, Shouts = ?, listeners = ? where trackID = ?; ",
                                                (t['playcount'],
                                                 t['total_shouts'],
                                                 t['listeners'],
                                                 t['id'])
            )
        self.conn.commit()



if __name__ == "__main__":
    week = int(sys.argv[1])
    action = sys.argv[2]
    if action == 'target':
        h = HistoryTable(week)
        h.update_targets()
    elif action == 'track':
        h = HistoryTable(week)
        h.update_tracks()

