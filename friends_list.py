# -*- coding: utf-8 -*-
from collections import namedtuple, defaultdict
from utils import Color, api_request, BatchRegulate
from functools import partial
import cPickle as pickle

all_friends = {}
friends_of_targets = defaultdict(list)

info_field = ['name', 'realname', 'url', 'id', 'country', 'age',
              'gender', 'subscriber', 'playcount', 'playlists',
              'bootstrap', 'registered', 'type', 'tags']

FriendInfo = namedtuple('FriendInfo', info_field )

def transfer_friend_info(info, tags):
    # python3 need this
    for k in info.keys()[:]:
        if k not in info_field:
            del info[k]
    for k in info_field:
        if k not in info:
            info[k] = None
    info['tags'] = tags
    return FriendInfo(**info)

def fetch_user_friends(username, limit=30):
    friends_dict = {}
    page = 1

    # make a request to get friends info
    method = "user.getFriends"
    params = {'user': username, 'limit': limit}

    while 1:
        params['page'] = page
        print Color.emphasise('--- user=%s friends page=%d ---' % (username, page))
        result = api_request(method, params=params)
        if result is not None and 'friends' in result:
            obj = result['friends']
        else:
            return None
        if page == 1 and 'user' not in obj:  # no friends
            return friends_dict
        else:
            attr = obj['@attr']
            last_page = int(attr['totalPages'])
            if isinstance(obj['user'], dict):
                u = obj['user']
                friends_dict[u['name']] = u
            else:
                for u in obj['user']:
                    friends_dict[u['name']] = u

            page += 1
            if page > last_page:
                # just return the result
                return friends_dict


def nameless(username, args):
    friend, info = args
    if friend not in all_friends:
        tags = get_user_tags(friend)
        all_friends[friend] = transfer_friend_info(info,tags)
    taste = get_tasteometer(username, friend)
    friends_of_targets[username].append((friend, taste[0], taste[1]))

def update_friends(username, index, all_):
    friends_dict = fetch_user_friends(username)
    if friends_dict is not None and len(friends_dict) != 0:
        # for friend, info in friends_dict.iteritems():
            # if friend not in all_friends:
                # tags = get_user_tags(friend)
                # all_friends[friend] = transfer_friend_info(info,tags)
            # taste = get_tasteometer(username, friend)
            # friends_of_targets[username].append((friend, taste[0], taste[1]))
        func = partial(nameless, username)
        obj = BatchRegulate(func, friends_dict.items(), cap=20)
        sum_ = len(friends_dict)
        count = 1
        for i in obj:
            print Color.ok("-------user=%s(%d/%d) friend=(%d/%d)------" % (
                            username, index, all_, count, sum_))
            count += 1
    else:
        friends_of_targets[username] = []


def get_user_tags(username):
    tags = []
    method = "user.getTopTags"
    params = {'user': username}
    result = api_request(method, params=params)
    if result is None:
        print Color.fail("----failed to get tags for %s----" % username)
    elif 'toptags' in result:
        result = result['toptags']
        if 'tag' in result:
            elem = result['tag']
            if isinstance(elem, list):
                for tag in elem:
                    tags.append((tag['name'], int(tag['count'])))
            else: # single dict
                tags.append((elem['name'], int(elem['count'])))
        else: # no tags
            print Color.emphasise("---- %s have no tags----" % username)

    return tags


def get_tasteometer(user1, user2):
    artists = []
    method = 'tasteometer.compare'
    params = {'type1': 'user', 'type2': 'user', 'value1': user1, 'value2': user2, 'limit': 10}
    result = api_request(method, params=params)
    score = 0.0
    if result is None:
        print Color.fail("----failed to compare %s and %s----" % (user1, user2))
    elif 'comparison' in result and 'result' in result['comparison']:
        result = result['comparison']['result']
        score = float(result['score'])
        arts = result['artists']
        if 'artist' in arts:
            elem = arts['artist']
            if isinstance(elem, list):
                for art in elem:
                    artists.append(art['name'])
            else: # single dict
                artists.append(elem['name'])
        else: #no common artist
            print Color.emphasise("---- %s and %s have common artist ----" % (user1, user2))

    return (score, artists)


if __name__ == "__main__":
    targets = pickle.load(open("data/week_b12/target_users.pkl"))
    l = len(targets)
    index = 1
    for t in targets:
        update_friends(t, index, l)
        index += 1
    with open("data/new_friends_info.pkl", "w") as f:
        pickle.dump(all_friends, f)
        pickle.dump(friends_of_targets, f)

