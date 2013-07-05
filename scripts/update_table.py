# coding:utf-8
import cPickle as pickle
import sqlite3
import subprocess
import os.path
from datetime import datetime


class HistoryTable(object):
    def __init__(self, week):
        self.week = week
        self.week_dir = "../data/week_%s/" % week
        print self.week_dir + "listen_history.db"
        self.conn = sqlite3.connect(self.week_dir + "listen_history.db")
        self.cur = self.conn.cursor()

    def update_targets(self):
        targets_file = self.week_dir + "target_users.pkl"
        if not os.path.exists(targets_file):
            print "no target update"
            return

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
        print "update targets complete"


    def update_tracks(self):
        tracks_file = self.week_dir + "tracks.pkl"


        if not os.path.exists(tracks_file):
            print "no tracks update"
            return
        tracks = pickle.load(open(tracks_file))
        for t in tracks:
            self.cur.execute("update tracks set trackPlayCnt = ?, Shouts = ?, listeners = ? where trackID = ?; ",
                                                (t['playcount'],
                                                 t['total_shouts'],
                                                 t['listeners'],
                                                 t['id'])
            )
        self.conn.commit()
        print "update tracks complete"

    def update_history(self):
        cmd = "echo '.dump history' |" + \
              " sqlite3 " + self.week_dir + "listened_history_week_%d_%d.db |" + \
              " sqlite3 " + self.week_dir + "listen_history.db"
        week_cmd = cmd % (self.week - 1, self.week)
        print week_cmd
        subprocess.call(week_cmd, shell=True)


    def update_table_if_need(self):
        pass

    def update_user_artist_stat(self):
        cmd = "cat user_artist_stat.sql |" + \
              " sqlite3 " + self.week_dir + "listen_history.db >" + \
              " " + self.week_dir + "user_artist_stat.txt"
        print cmd
        if subprocess.call(cmd, shell=True) is 0:
            return True
        else:
            print "export user artist statistic error"
            return False

    def update_all(self):
        self.update_history()
        self.update_tracks()
        if self.update_user_artist_stat():
            self.update_targets()


if __name__ == "__main__":
    week = 25
    h = HistoryTable(week)
    h.update_all()


