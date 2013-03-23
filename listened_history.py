# -*- coding: utf-8 -*-
import requests
import sqlite3
from utils import mild_request, api_request, dom_request, gevent_do, save, BatchRegulate
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
    create table if not exists meta_info (
        key primary key,
        value
    );
""")

class History(object):
    method = "user.getRecentTracks"
    insert_sql = "insert into history values (?, ?, ?, ?, ?, ?, ?, ?)"

    def __init__(self, username):
        self.username = username
        self.params = {'user': username, 'extend':1}
    
    def request_first(self):
        params = {"limit": 1, "page": 1}
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


    def request_page(self, page):
        params = self.params
        params['page'] = page
        r = api_request(self.method, params)
        if r is None or "recenttracks" not in r:
            return None
        result = r["recenttracks"]["track"]
        return result

    def convert_recent_info(self, recent_track):
        user = self.username
        track = recent_track['name']
        artist = recent_track['artist']['#text']
        streamable = recent_track['streamable']
        album = recent_track['album']['#text'] if 'album' in recent_track else None
        url = recent_track['url']
        loved = recent_track['loved'] if 'loved' in recent_track else 0
        datetime = recent_track['date']['uts']
        return (user, track, artist, streamable, album, url, loved, datetime)

    def write_to_db(self, recent_tracks):
        if isinstance(recent_tracks, list):
            cursor.executemany(self.insert_sql, map(self.convert_recent_info, recent_tracks))
        elif isinstance(recent_tracks, dict):
            cursor.execute(self.insert_sql, self.convert_recent_info(recent_tracks))



def wirte_user_history(username, page=1):
    history = History(username)
    result = history.request_first()
    if not result:
        print "--- no history of %s ---" % username
        return

    if '@attr' in result:
        meta = result['@attr']
    else:
        meta = result
        
    total_page = int(meta["totalPages"].strip())
    total = int(meta["total"].strip())
    print "--- totalpage = %d total = %d ---" % (total_page, total)

    page_to_fetch = []
    while page <= total_page:
        page_to_fetch.append(page)

        if len(page_to_fetch) == 5 or page == total_page:
            results = gevent_do(history.request_page, page_to_fetch)
            print "--- a round of fetchs complete! ---"
            for rsut in results:
                if not rsut:
                    continue
                history.write_to_db(rsut)
                
            # save every 10 pages
            cursor.execute('insert or replace into meta_info values ("user", ?)', (username,))
            cursor.execute('insert or replace into meta_info values ("page", ?)', (page,))
            conn.commit()

            # collect next round
            del page_to_fetch[:]

        page += 1

def count_total_record():
    # target_friends = cPickle.load(open("data/friends_info.pkl", "rb"))
    # friends_count = len(target_friends)
    # users = set(target_friends.keys())
    targets = cPickle.load(open("data/target_users.pkl"))
    target_count = len(targets)
    users = []
    users.extend(targets.keys())
    # users = list(users)
    # save memory
    # target_friends = None
    targets = None
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
        print "count %d users --- count = %d ---" % (user_count, count)
    # print "friends_count = %d" % friends_count
    print "target_count = %d" % target_count
    return count


if __name__ == "__main__":
    wirte_user_history('RJ')
    # count_total_record()
