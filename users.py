# -*- coding: utf-8 -*-
import requests
from collections import deque
from utils import mild_request, api_request, dom_request, gevent_do, save, BatchRegulate
from pyquery import PyQuery as pq
import random
import json
import sys
import os.path
import socket
import cPickle
import time
import functools

USERS_POOL = set()
FIFO_QUEUE = deque()
USER_INFO_LOG_FILE = None
USER_FRIENDS_LOG_FILE = None
DEBUG = True
seed_user = 'RJ'


def get_user_info(username):
    method = "user.getInfo"
    params = {'user': username}
    return api_request(method, params)['user']


def get_user_friends(username, limit=50, page=0):
    friends_dict = {}
    fetch_all = False

    if page == 0:
        page = 1
        fetch_all = True

    # make a request to get friends info
    method = "user.getFriends"
    params = {'user': username, 'limit': limit}

    while True:
        params['page'] = page
        if DEBUG:
            print '--- user=%s friends page=%d ---' % (username, page)
        result = api_request(method, params=params)
        if result is not None and 'friends' in result:
            obj = result['friends']
        else:
            return None
        if page == 1 and 'user' not in obj: # no friends
            return (friends_dict, 0)
        else:
            attr = obj['@attr']
            total_friends = int(attr['total'])
            last_page = int(attr['totalPages'])
            if total_friends > 200:
                # pass this user
                return None
            if isinstance(obj['user'], dict):
                u = obj['user']
                friends_dict[u['name']] = u 
            else:
                for u in obj['user']:
                    friends_dict[u['name']] = u
            
            page += 1
            if len(friends_dict) > 1000 or not fetch_all or page > last_page:
                return (friends_dict, total_friends)
                break



# def handle_each_user(username):
    # print "--- now for user=%s ---" % username
    # friends_info_dict = get_user_friends(username)
    # friends_list = map(str, friends_info_dict.keys())
    # friends_set = set(friends_list)
    # new_user = friends_set - USERS_POOL
    # old_user = friends_set - new_user
    # for u in old_user:
        # del friends_info_dict[u]
    # USERS_POOL.update(new_user)
    # FIFO_QUEUE.extend(new_user)
    # log_user_friends(username, friends_list,
            # USER_FRIENDS_LOG_FILE)
    # log_friends_info(friends_info_dict, USER_INFO_LOG_FILE)
    # return new_user


# def get_recent_tracks(username, page=0, limit=10, extended=1):
    # url = API_REQUEST_URL % "user.getRecentTracks"
    # if page == 0:
        # page = 1
        # fetch_all = True
    # params = {'user': username, 'limit': limit, "page": page, "extended": extended}


# def persistence():
    # objs = {'pool': USERS_POOL, 'queue': FIFO_QUEUE}
    # file_name = "data.pkl"
    # f = open(file_name, "w")
    # cPickle.dump(objs, f, 2)
    # f.close()


# def reproduct():
    # queue = FIFO_QUEUE
    # pool = USERS_POOL
    # num_logged_info_users = len(USERS_POOL)
    # while len(queue):
        # if num_logged_info_users > 100000:
            # print "The users is enough!"
            # break
        # head = queue.popleft()
        # head = str(head)  # json.loads get unicode
        # log all new user info in friends and log all friends 
        # num_logged_info_users += len(handle_each_user(head))
        # persistence()
        # if DEBUG:
            # print "--- now have %d user_infos ---" % num_logged_info_users
    # if DEBUG and not len(queue):
        # print "--- converged!!! ---"
    # if num_logged_info_users > 100000:
        # persistence()


def get_listening_now_users(track_url):
    url = track_url + '/+listeners'
    dom = dom_request(url)
    li_list = dom(".usersMedium:eq(0) > li")
    user_list = li_list.map(lambda i, e: pq(e)("a:eq(0)").text())
    return user_list

def get_seed_users(tracks):
    seed_users = set()
    tmp = []
    for i, t in enumerate(tracks, start=1):
        tmp.append(t["url"])
        if len(tmp) == 10 or i == len(tracks):
            new_seeds = gevent_do(get_listening_now_users, tmp)
            for group in new_seeds:
                seed_users.update(set(group))
            tmp = []
    return seed_users

def get_target_users(seed_users):
    target_users_info = {}
    limit = 50
    new_func = functools.partial(get_user_friends, limit=limit, page=1)
    bobj = BatchRegulate(new_func, list(seed_users))
    for user in seed_users:
        # use gevent
        result = next(bobj)
        if result:
            (friends_info, total) = result
        else:
            continue
        # python 2.7, target not in seed
        friends_info = {k:v for k, v in friends_info.iteritems() if k not in seed_users}
        if not friends_info:
            continue
        # select only one target
        # key = random.choice(friends_info.keys())
        # target_users_info.update({key: friends_info[key]})
        # select 6 friends
        if len(friends_info) >= 6:
            keys = random.sample(friends_info, 6)
            friends_info = {k:v for k, v in friends_info.iteritems() if k in keys}
        target_users_info.update(friends_info)


    return target_users_info


def log_seed_users(users, seed_users_log_file):
    if isinstance(users, list):
        for u in users:
            print >> seed_users_log_file, u
    elif isinstance(users, (str, unicode)):
        print >> seed_users_log_file, users

def log_target_users():
    pass

def log_user_info(user_info, userinfo_log_file):
    print >> userinfo_log_file, user_info

def log_user_friends(user, friends, friends_log_file):
    for f in friends:
        print >> friends_log_file, user, f

def log_friends_info(user_info_dict, user_info_log_file):
    for u in user_info_dict:
        print >> user_info_log_file, json.dumps(user_info_dict[u])

def filter_target_info(user_info):
    gender = user_info['gender'].strip()
    age = user_info['age'].strip()
    if gender == "" or gender == "n":
        return False
    if age == "":
        return False
    return True

def prepare_target_users():
    target_data = "data/target_users.pkl"
    seed_data = "data/seed_users.pkl"

    if os.path.exists(target_data):
        targets_info = cPickle.load(open(target_data))
    else:
        if not os.path.exists(seed_data):
            tracks = cPickle.load(open("data/recent_tracks.pkl", "rb"))
            seeds = get_seed_users(tracks)
            print "total %d seed users" % len(seeds)
            save(seed_data, seeds)
        else:
            seeds = cPickle.load(open(seed_data))
        targets_info = get_target_users(seeds)
        targets_info = {k:v for k, v in targets_info.iteritems() if filter_target_info(v)}
        print "total %d target users" % len(target_data)
        save(target_data, targets_info)
    return targets_info

def get_target_friends(targets_info):
    target_data = "data/target_users.pkl"

    target_friends = {}
    friends_info = {}
    invalid_targets = []


    count = 0
    for name in targets_info:
        result = get_user_friends(name)
        if result is not None:
            targets_info[name]['friend_nums'] = len(result[0])
            target_friends[name] = result[0].keys()
            friends_info.update(result[0])
            count += 1
            if count == 1000:
                break
        else:
            invalid_targets.append(name)

    for name in targets_info.keys():
        if name not in target_friends:
            del targets_info[name]

    for name in invalid_targets:
        del targets_info[name]

    # if len(targets_info) > 1000:
        # keys = random.sample(targets_info, 1000)
        # targets_info = {k:v for k, v in targets_info.iteritems() if k in keys}

    save(target_data, targets_info)
    print "--- total %d targets ---" % len(targets_info)
    save("data/target_friends.pkl", target_friends)
    count = sum(len(fr) for fr in target_friends.itervalues())
    print "--- all targets have %d friend counts ---" % count
    save("data/friends_info.pkl", friends_info)
    print "--- total %d friends ---" % len(friends_info)


def update_targets(week):
    target_week_data = "data/" + ("week_%/" % week) + "target_users.pkl"
    update_info = {}
    with open("data/target_users.pkl") as orig:
        targets = cPickle.load(orig)
        for t in targets:
            update_info[t] = get_user_info(t)

    save(update_info, target_week_data)
    print "---- update to file %s %d targets---" % (target_week_data, len(update_info))
    return update_info


if __name__ == "__main__":
    # if  False:
        # file_name = "data.pkl"
        # USER_INFO_LOG_FILE = open("user_info.txt", "a")
        # USER_FRIENDS_LOG_FILE = open("user_friends.txt", "a")

        # if os.path.exists(file_name):
            # restore last run
            # with open(file_name) as f:
                # objs = cPickle.load(f)
                # USERS_POOL = objs['pool']
                # FIFO_QUEUE = objs['queue']
        # else:
            # pool = USERS_POOL
            # queue = FIFO_QUEUE
            # pool.add(seed_user)
            # queue.append(seed_user)
        # try:
            # reproduct()
        # except:
            # USER_FRIENDS_LOG_FILE.flush()
            # USER_INFO_LOG_FILE.flush()
            # raise
    # target_data = "data/target_users.pkl"
    # seed_data = "data/seed_users.pkl"

    # if True:
        # targets_info = prepare_target_users()

        # target_friends = {}
        # friends_info = {}
        # invalid_targets = []


        # count = 0
        # for name in targets_info:
            # result = get_user_friends(name)
            # if result is not None:
                # targets_info[name]['friend_nums'] = len(result[0])
                # target_friends[name] = result[0].keys()
                # friends_info.update(result[0])
                # count += 1
                # if count == 1000:
                    # break
            # else:
                # invalid_targets.append(name)

        # for name in targets_info.keys():
            # if name not in target_friends:
                # del targets_info[name]

        # for name in invalid_targets:
            # del targets_info[name]

        # # if len(targets_info) > 1000:
            # # keys = random.sample(targets_info, 1000)
            # # targets_info = {k:v for k, v in targets_info.iteritems() if k in keys}

        # save(target_data, targets_info)
        # print "--- total %d targets ---" % len(targets_info)
        # save("data/target_friends.pkl", target_friends)
        # count = sum(len(fr) for fr in target_friends.itervalues())
        # print "--- all targets have %d friend counts ---" % count
        # save("data/friends_info.pkl", friends_info)
        # print "--- total %d friends ---" % len(friends_info)
    if True:
        update_targets(13)
