# coding: utf-8

from utils import api_request, save, iter_pool_do, get_track_releasetime
from utils import request_url, Color
from functools import wraps
import cPickle as pickle
import sqlite3
import os.path
import functools
import time



def friend_like(track, artist, friend):
    params = {'username': friend, 'track': track, 'artist': artist}
    service = 'track.getInfo'
    result = api_request(service, params)

    try:
        track = result['track']
        if 'userplaycount' not in track:
            return (0, 0)
        userplaycount = track['userplaycount']
        userloved = track['userloved']
        return (userplaycount, userloved)
    except (TypeError, KeyError):
        print "--- get wired result in friend_like (%s, %s, %s) ---" % (
                track, artist, friend)
        return None


def get_tracks():
    tracks_file = 'data/tracks_info.pkl'
    tracks_info = pickle.load(open(tracks_file))
    tracks = sorted((t['name'], t['artist']['name']) for t in tracks_info)
    return tracks

def get_targets():
    targets_file = 'data/target_users.pkl'
    targets_info = pickle.load(open(targets_file))
    targets = sorted(targets_info.keys())
    return targets


def decorator(func):
    friends_cache = pickle.load(open("data/week_b12/target_friends.pkl"))
    @wraps(func)
    def wrapper(target):
        return friends_cache[target]

    return wrapper

@decorator
def get_target_friends(target):
    pass



def get_playcount_and_love():
    CONN = sqlite3.connect('data/friends_listened.db')
    CURSOR = CONN.cursor()
    CURSOR.executescript("""
        create table if not exists playcount_and_love (
            target,
            friendname,
            track,
            artist,
            playcount,
            loved,
            timestamp
        );
    """)

    targets = get_targets()
    tracks = get_tracks()

    save_file = 'save_for_friend.pkl'
    error_file = open('error_file.txt', 'a')
    if os.path.exists(save_file):
        obj = pickle.load(open(save_file))
        last_index1 = obj['index1']
        next_index2 = obj['index2']
        already_fetched = obj['already']
    else:
        last_index1 = 0
        next_index2 = 0
        already_fetched = set()

    for index1, t in enumerate(tracks[last_index1:], start=1+last_index1):
        track = t[0]
        artist = t[1]

        for index2, target in enumerate(targets[next_index2:], start=1+next_index2):
            friends = get_target_friends(target)
            filted_friends = [f for f in friends if f not in already_fetched]

            # for index3, friend in enumerate(friends, start=1):
            func = functools.partial(friend_like, track, artist)
            generator = iter_pool_do(func, filted_friends, cap=10)
            index3 = 1
            for friend, result in generator:
                # if friend in already_fetched:
                    # # skip this one
                    # continue
                print "--- [%s(%d:%d) %s(%d:%d) %s(%d:%d)] ---" % (
                        track, index1, len(tracks),
                        target, index2, len(targets),
                        # friend, index3, len(friends))
                        friend, index3, len(filted_friends))
                # result = friend_like(track, artist, friend)
                if result:
                    # insert in to db
                    playcount, loved = result
                    if playcount:
                        print "--- get valid record! ---"
                        CURSOR.execute(
                            "insert into playcount_and_love values (?, ?, ?, ?, ?, ?, ?)",
                            (
                                target,
                                friend,
                                track,
                                artist,
                                playcount,
                                loved,
                                int(time.time())
                            )
                        )
                    else:
                        already_fetched.add(friend)
                else:
                    # log this
                    print >> error_file, "(%s) (%s) (%s)" % (track.encode('utf-8'),
                                                             target.encode('utf-8'),
                                                             friend.encode('utf-8'))
                    error_file.flush()
                index3 += 1

            # save what?
            # in second loop
            # next (track, targets)
            # already_list for current track
            save_obj = {
                'index1': index1-1,
                'index2': index2,
                'already': already_fetched
            }
            save(save_file, save_obj)
            CONN.commit()

        # prepare for next track
        already_fetched = set()
        next_index2 = 0


def get_all_friends():
    target_friends = pickle.load(open("data/week_b12/target_friends.pkl"))
    all_friends = set()
    for t, fs in target_friends.iteritems():
        all_friends.update(fs)
    return all_friends


class History(object):
    method = "user.getRecentTracks"
    insert_sql = "insert into history values (?, ?, ?, ?, ?, ?, ?, ?)"
    per_page = 30
    total_user = None
    logf = None
    debug = False
    cursor = None
    conn = None

    def __init__(self, username, tr_from=None, tr_to=None, target=1, index=None):
        # target stand for target page, this is a bad name
        self.username = username
        self.params = {'user': username}
        if tr_from:
            self.params['from'] = tr_from
        if tr_to:
            self.params['to'] = tr_to
        self.target = target
        self.index = index

    def get_count(self):
        params = {"limit": 1, "page": 1}
        params.update(self.params)
        result = api_request(self.method, params)
        if result is None:
            return 0
        if "status" in result and result["status"] == 'ok':
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
        params = {"limit": self.per_page, "page": page, 'extend': 1}
        params.update(self.params)
        r = api_request(self.method, params)
        if r is None or "recenttracks" not in r:
            return None

        if '@attr' in r["recenttracks"]:
            meta = r["recenttracks"]["@attr"]
        else:
            meta = r["recenttracks"]
        if int(meta['page']) != page:
            raise RuntimeError

        if "track" in r["recenttracks"]:
            result = r["recenttracks"]["track"]
            if isinstance(result, dict):  # only one item
                result = [result]
        else:
            result = None
        return result

    def log_this(self, page):
        params = {"limit": self.per_page, "page": page, 'extend': 1}
        params.update(self.params)
        print "--- %s ---" % Color.fail("error request")
        if self.debug is False:
            print >> History.logf, request_url(self.method, params)
            History.logf.flush()

    def request(self):
        target = self.target
        current = target - 1
        # if the count is 0, record wouldn't be deleted
        total = self.get_count()
        # this num isn't correct
        total_pages = (total-1) / self.per_page + 1
        alist = []
        while current < total:
            next_page, offset = divmod(current, self.per_page)
            next_page += 1
            try:
                result = self.get_page(next_page)
                print "---user=%s(%d:%d) page(%d:%d) ---" % (
                        self.username, self.index, History.total_user,
                        next_page, total_pages)
            except RuntimeError:
                break
            if result is not None:
                alist.extend(result[offset:])
            else:
                self.log_this(next_page)

            current += (self.per_page - offset)
            if len(alist) >= 100:
                target = current + 1
                self.update_db(alist, target)
                del alist[:]

        if alist:
            # delete target record
            self.update_db(alist)

        return True

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

    def update_db(self, alist, target=None):
        if self.debug == True:
            return
        print "--- %s %d records to db ---" % (Color.ok("update") ,len(alist))
        History.cursor.executemany(self.insert_sql,
                           map(self.convert_recent_info, alist))
        if target is None:
            History.cursor.execute("delete from meta_info where name = ?",
                                   (self.username,))
        else:
            History.cursor.execute(
                    "update meta_info set target = ? where name = ?",
                    (target, self.username))
        History.conn.commit()


def prepare_history_db(filename):
    conn = sqlite3.connect('data/friends_listened_history.db')
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
            name,
            left_time,
            right_time,
            target
        );
    """)
    cursor.execute("select count(*) from meta_info;")
    count1 = cursor.fetchone()[0]
    cursor.execute("select count(*) from history;")
    count2 = cursor.fetchone()[0]
    # no record, it means run from scratch
    if count1 == 0 and count2 == 0:
        for l in open("data/time_range/" + filename):
            l = l.decode('utf-8').strip()
            tmp = l.split('|')
            tmp.append(1)
            cursor.execute("insert into meta_info values (?, ?, ?, ?)",
                               tuple(tmp))
        conn.commit()

    History.conn = conn
    History.cursor = cursor
    return (cursor, conn)


def restore_from_db(cursor):
    alist = []
    cursor.execute("select * from meta_info;")
    for row in cursor:
        alist.append(row)
    return alist


def dispatch_one_user(args):
    index, param = args
    return History(*param, index=index).request()


def get_friends_history(filename):
    cursor, conn = prepare_history_db(filename)
    ranges = restore_from_db(cursor)
    History.total_user = len(ranges)
    range_with_index = list(enumerate(ranges, start=1))
    gen = iter_pool_do(dispatch_one_user, range_with_index, cap=10)
    for g in gen:
        pass


if __name__ == '__main__':
    LOG_FILE = "log/friends_history.txt"
    History.debug = False
    History.logf = open(LOG_FILE, "a")
    get_friends_history("user03")
    History.logf.flush()
