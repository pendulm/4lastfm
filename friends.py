# coding: utf-8

from utils import api_request, save, iter_pool_do
from functools import wraps
import cPickle as pickle
import sqlite3
import os.path
import functools
import time

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



if __name__ == '__main__':
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
            generator = iter_pool_do(func, filted_friends)
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
                    print >> error_file, "(%s) (%s) (%s)" % (track, target, friend)
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

