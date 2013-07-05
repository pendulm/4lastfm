# coding: utf-8

from utils import api_request, save, iter_pool_do, get_track_releasetime
from utils import request_url, Color, DBWrapper, simple_cache
from utils import timestamp_of_nth_week
from collections import defaultdict
import cPickle as pickle
import os.path
import sys
import math
import itertools

class FriendHistory(object):
    METHOD = "user.getRecentTracks"

    @classmethod
    def setup(cls, db, total, log_file=sys.stdout, debug=False):
        cls.db = db
        cls.total = total
        cls.log_file = log_file
        cls.debug = debug
        cls.per_page = 30
        cls.count = 0

    def __init__(self, username, tr_from=None, tr_to=None):
        self.username = username
        self.params = {'user': username}
        if tr_from:
            self.params['from'] = tr_from
        if tr_to:
            self.params['to'] = tr_to
        FriendHistory.count += 1
        self.index = FriendHistory.count

    def get_count(self):
        params = {"limit": 1, "page": 1}
        params.update(self.params)
        result = api_request(self.METHOD, params)
        if result is None:
            return 0
        if "status" in result and result["status"] == 'ok':
            # take a breath and retry
            gevent.sleep(2)
            return self.get_count()
        elif 'recenttracks' in result:
            if '@attr' in result["recenttracks"]:
                meta = result["recenttracks"]["@attr"]
            else:
                meta = result["recenttracks"]
            return int(meta["total"].strip())
        else:
            return 0

    def get_page(self, page):
        params = {"limit": FriendHistory.per_page, "page": page, 'extend': 1}
        params.update(self.params)
        r = api_request(FriendHistory.METHOD, params)
        if r is None or "recenttracks" not in r:
            return None

        if '@attr' in r["recenttracks"]:
            meta = r["recenttracks"]["@attr"]
        else:
            meta = r["recenttracks"]

        if int(meta['page']) != page:
            # so api error
            raise RuntimeError

        if "track" in r["recenttracks"]:
            result = r["recenttracks"]["track"]
            if isinstance(result, dict):  # only one item
                result = [result]
        else:
            result = None

        if result is None:
            self.log_this(page)
        return result

    def log_this(self, page):
        params = {"limit": FriendHistory.per_page, "page": page, 'extend': 1}
        params.update(self.params)
        if FriendHistory.debug is True:
            # write to screen
            print "--- %s ---" % Color.fail("error request")
        print >> FriendHistory.log_file, request_url(self.METHOD, params)
        FriendHistory.log_file.flush()

    def request(self):
        total_records = self.get_count()
        total_pages = int(math.ceil(total_records / FriendHistory.per_page))
        wanted_page = 1

        alist = []
        while wanted_page <= total_pages:
            try:
                result = self.get_page(wanted_page)
                print "---user=%s(%d:%d) page(%d:%d) ---" % (
                        self.username, self.index, FriendHistory.total,
                        wanted_page, total_pages)
            except RuntimeError:
                # get page number not match which we specified
                break

            if result is not None:
                alist.extend(result)

            if len(alist) >= 300:
                self.update_db(alist)
                del alist[:]

            wanted_page += 1

        # delete this friend progress
        self.update_db(alist, True)

    def update_db(self, alist, delete=False):
        filtered_list = []
        for record in alist:
            track_id = self.filter_record(record)
            if track_id is not None:
                for target in build_revert_relation()[self.username]:
                    target_id = build_targets_hash()[target]
                    filtered_list.append((target_id, track_id))

        if filtered_list:
            print "--- %s %d records to db ---" % (Color.ok("update") ,len(filtered_list))
            if FriendHistory.debug is False:
                FriendHistory.db.executemany(
                        "update friend_listeners set FriendsListenerNum = FriendsListenerNum + 1 where target_id == ? and track_id == ?;", filtered_list)

        if FriendHistory.debug is True:
            # do not actually touch db
            return

        if delete:
            FriendHistory.db.execute("delete from meta_info where friend = ?",
                                   (self.username,))
        else:
            if alist:
                last_timestamp = alist[-1]["date"]["uts"]
                FriendHistory.db.execute(
                        "update meta_info set timestamp = ? where friend = ?",
                        (last_timestamp, self.username))
        FriendHistory.db.commit()

    def filter_record(self, record):
        name = record['name']
        artist = record['artist']['#text']
        tracks_hash = build_tracks_hash()
        key = (name, artist)
        if key in tracks_hash:
            return tracks_hash[key]
        else:
            return None


@simple_cache
def build_revert_relation():
    rfile = "data/target_friends.pkl"
    target_friends = pickle.load(open(rfile))
    revert_relation = defaultdict(list)

    for target, friends in target_friends.iteritems():
        for friend in friends:
            revert_relation[friend].append(target)

    return revert_relation


@simple_cache
def build_targets_hash():
    tfile = "data/target_users.pkl"
    targets = pickle.load(open(tfile))
    return {t: targets[t]['id'] for t in targets}

@simple_cache
def build_tracks_hash():
    tfile = "data/tracks_info.pkl"
    tracks = pickle.load(open(tfile))
    return {(t['name'], t['artist']['name']): t['id'] for t in tracks}


def create_friend_listeners_table(week):
    dbfile = ("data/week_%d/" % week) + "friend_listeners.db"
    db = DBWrapper(dbfile)
    db.executescript("""
            create table if not exists friend_listeners (
                target_id,
                track_id,
                FriendsListenerNum
            );
            create table if not exists meta_info (
                friend,
                timestamp
            );""")

    db.commit()
    return db


def initialize_friend_listeners_table(week, db):
    targets_id = build_targets_hash().values()
    tracks_id = build_tracks_hash().values()
    for target_id in targets_id:
        for track_id in tracks_id:
            db.execute("insert into friend_listeners values (?, ?, ?)",
                         (target_id, track_id, 0))

    friends = build_revert_relation().keys()
    start_time = timestamp_of_nth_week(week - 1)

    for friend in friends:
        db.execute("insert into meta_info values (?, ?)", (friend, start_time))

    db.commit()


def dispatch_one_user(args):
    return FriendHistory(*args).request()


def load_progress(db, week):
    end_time = timestamp_of_nth_week(week)
    cur = db.execute("select friend, timestamp, %d from meta_info;" % end_time )
    return [row for row in cur]


def scheduling_scrape(week):
    db = create_friend_listeners_table(week)
    count = db.execute("select count(*) from meta_info;").fetchone()[0]
    if count == 0:
        initialize_friend_listeners_table(week, db)
    progress = load_progress(db, week)
    log_file = open("data/week_%s/friends.log" % week, "a")
    total = len(progress)
    FriendHistory.setup(db, total, log_file)

    gen = iter_pool_do(dispatch_one_user, progress, cap=10)
    for g in gen:
        pass
    # all_count = 0
    # index = 1
    # for key, reslt in gen:
        # if reslt:
            # all_count += reslt
            # print Color.ok("--- (%d/%d) %d %d ---" % (index, total, reslt, all_count))
        # index += 1


if __name__ == "__main__":
    week = 24
    scheduling_scrape(week)
