# coding: utf-8

from utils import api_request
from functools import wraps
import cPickle as pickle


def friend_like(friend, trackinfo):
    track = trackinfo['name']
    artist = trackinfo['artist']['name']
    params = {'username': friend, 'track': track, 'artist': artist}
    service = 'track.getInfo'
    result = api_request(service, params)

    try:
        track = result['track']
        if 'userplaycount' not in track:
            return None
        userplaycount = track['userplaycount']
        userloved = track['userloved']
        return (userplaycount, userloved)
    except (TypeError, KeyError):
        print "--- get wired result in friend_like (%s, %s) ---" % (friend,
                track)
        return None


def get_tracks():
    tracks_file = 'data/tracks_info.pkl'
    tracks = pickle.load(open(tracks_file))
    return tracks

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
    targets = pickle.load(open("data/target_users.pkl"))
    tracks = get_tracks()
    for index1, t in enumerate(tracks, start=1):
        name = t['name']
        artist = t['artist']['name']
        already_fetched = set()
        for index2, tar in enumerate(targets, start=1):
            friends = get_target_friends(tar)
            for index3, f in enumerate(friends, start=1):
                if f in already_fetched:
                    # skip this one
                    continue
                print "--- now for [%s(%d:%d) %s(%d:%d) %s(%d:%d)] ---" % (
                        name, index1, len(tracks),
                        tar, index2, len(targets),
                        f, index3, len(friends))
                result = friend_like(f, t)
                if result:
                    # insert in to db
                    print "--- get valid record! ---"
                    already_fetched.add(f)
