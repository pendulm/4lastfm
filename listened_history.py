# -*- coding: utf-8 -*-
import requests
import sqlite3
from utils import mild_request, api_request, dom_request, gevent_do
from utils import save, BatchRegulate, pool_do, timestamp_of_nth_week
from pyquery import PyQuery as pq
import random
import json
import sys
import os.path
import socket
import cPickle
import time

conn = sqlite3.connect('data/listen_history.db')
cursor = conn.cursor()
cursor.executescript("""
    create table if not exists history (
        user,
        track,
        artist,
        streamable,
        album,
        url,
        loved,
        datetime
    );
""")
progresses = {}
progresses_file = "data/progress.pkl"
toatl_users = 0

class History(object):
    method = "user.getRecentTracks"
    insert_sql = "insert into history values (?, ?, ?, ?, ?, ?, ?, ?)"
    time_range = [None, None]

    def __init__(self, username):
        self.username = username
        self.params = {'user': username, 'extend':1}
        if self.time_range[0]:
            self.params['from'] = self.time_range[0]
        if self.time_range[1]:
            self.params['to'] = self.time_range[1]
    
    def request_first(self, limit=1):
        params = {"limit": limit, "page": 1}
        params.update(self.params)
        result = api_request(self.method, params)
        if result is None:
            return None
        if "status" in result and result["status"] == 'ok':
            gevent.sleep(5)
            return self.request_first()
        elif 'recenttracks' in result:
            return result["recenttracks"]
        else:
            return None

    def get_history_count(self):
        result = self.request_first()
        if not result:
            return 0

        if '@attr' in result:
            meta = result['@attr']
        else:
            meta = result

        total = int(meta["total"].strip())
        return total


    def request_page(self, page, limit=50):
        params = self.params
        params['page'] = page
        params['limit'] = limit
        r = api_request(self.method, params)
        if r is None or "recenttracks" not in r:
            return None
        result = r["recenttracks"]["track"] if "track" in r["recenttracks"] else None
        return result

    def convert_recent_info(self, recent_track):
        user = self.username
        track = recent_track['name'] if 'name' in recent_track else None
        artist = recent_track['artist']['#text'] if 'artist' in recent_track and '#text' in recent_track['artist'] else None
        streamable = recent_track['streamable'] if 'streamable' in recent_track else None
        album = recent_track['album']['#text'] if 'album' in recent_track else None
        url = recent_track['url'] if 'url' in recent_track else None
        loved = recent_track['loved'] if 'loved' in recent_track else 0
        datetime = recent_track['date']['uts'] if 'date' in recent_track and 'uts' in recent_track['date'] else None
        return (user, track, artist, streamable, album, url, loved, datetime)

    def write_to_db(self, recent_tracks):
        if isinstance(recent_tracks, list):
            cursor.executemany(self.insert_sql, map(self.convert_recent_info, recent_tracks))
            return len(recent_tracks)
        elif isinstance(recent_tracks, dict):
            cursor.execute(self.insert_sql, self.convert_recent_info(recent_tracks))
            return 1



def write_user_history(progress, count=[0]):
    # count is static variable
    count[0] += 1
    now_count = count[0]
    username, page = progress
    history = History(username)
    # get total page
    result = history.request_first(limit=50)
    if not result:
        print "--- no history of %s ---" % username
        return 0

    if '@attr' in result:
        meta = result['@attr']
    else:
        meta = result

    total_page = int(meta["totalPages"].strip())
    total = int(meta["total"].strip())
    print "--- totalpage = %d total = %d ---" % (total_page, total)

    # page_to_fetch = []
    record_count = 0
    problem_record = []
    while page <= total_page:
        # page_to_fetch.append(page)

        # if len(page_to_fetch) == 5 or page == total_page:
            # results = gevent_do(history.request_page, page_to_fetch)
            # print "--- a round of fetchs complete! ---"
            # for rsut in results:
                # if not rsut:
                    # continue
                # history.write_to_db(rsut)
        result = history.request_page(page, limit=50)
        print "--- now get user=%s(%d:%d/%d) page=(%d, %d) ---" % (
                username, now_count, count[0], toatl_users, page, total_page)
        if result:
            record_count += history.write_to_db(result)
        else:
            # problem url
            problem_record.append(page)


        # save every 5 pages
        if (page % 5) == 0:
            # cursor.execute('insert or replace into meta_info values ("user", ?)', (username,))
            # cursor.execute('insert or replace into meta_info values ("page", ?)', (page,))
            conn.commit()
            progresses[username] = page
            save(progresses_file, progresses)

            # # collect next round
            # del page_to_fetch[:]

        page += 1
    # finish this user so do a snapshot
    conn.commit()
    del progresses[username]
    save(progresses_file, progresses)
    # return record_count
    return problem_record

def count_total_record(users):
    # target_friends = cPickle.load(open("data/friends_info.pkl", "rb"))
    # friends_count = len(target_friends)
    # users = set(target_friends.keys())
    # targets = cPickle.load(users_file)
    users_num = len(users)
    # users = []
    # users.extend(targets.keys())
    # users = list(users)
    # save memory
    # target_friends = None
    # targets = None
    count = 0
    user_count = 0
    bobj = BatchRegulate(lambda u: History(u).get_history_count(), users, 5)
    for user in users:
        user_count += 1
        # history = History(user)
        # count += history.get_history_count()
        n = next(bobj)
        if n is None:
            n = 0
        count += n
        test = user_count % 1000
        if test == 0:
            print "--- take a breath! ---"
            time.sleep(60)
        print "count (%d/%d) users=%s listened=%d --- tracks count = %d ---" % (
                user_count, users_num, user, n, count)
    # print "friends_count = %d" % friends_count
    # print "target_count = %d" % target_count
    return count

def get_week_range_history(start, end):
    targets_file = "data/targets_1.pkl"
    if os.path.exists(progresses_file):
        # restore last progress
        progresses = cPickle.load(open(progresses_file))
    else:
        # run from start
        users = cPickle.load(open(targets_file))
        progresses = {u:1 for u in users}
    toatl_users = len(progresses)

    # the 12th week of 2013 is 2013/03/18, get all recent record before this point
    History.time_range[1] = timestamp_of_nth_week(12)
    user_record = pool_do(write_user_history, progresses.items(), cap=2)
    print "total %d track records" % sum(user_record.itervalues())

def redo_for_not_tracked_users():
    users = ['ElinaKubulina', 'green_aglets']
    History.time_range[1] = timestamp_of_nth_week(12)
    progresses = {u:1 for u in users}
    problem_results = pool_do(write_user_history, progresses.items(), cap=1)


if __name__ == "__main__":
    # wirte_user_history('RJ')
    # users_file = open("data/targets_1.pkl")
    # users = cPickle.load(users_file)
    # count_total_record(users)
    redo_for_not_tracked_users()

