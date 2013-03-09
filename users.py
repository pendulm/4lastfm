# -*- coding: utf-8 -*-
import requests
from collections import deque
import json
import sys
import os.path
import socket
import cPickle
import time

API_REQUEST_URL = 'http://ws.audioscrobbler.com/2.0/?method=%s&api_key=9edee2e7f91969898fa60945cd818b55&format=json'

USERS_POOL = set()
FIFO_QUEUE = deque()
USER_INFO_LOG_FILE = None
USER_FRIENDS_LOG_FILE = None
DEBUG = True
seed_user = 'RJ'


def mild_request(url, params={}):
    timeout = 5
    try_num = 1
    while True:
        try:
            r = requests.get(url, timeout=timeout, params=params)
            # print r.url
            if DEBUG and try_num > 1:
                print "--- retry number = %d ---" % try_num
            result = r.json()
            if "error" in result and result["error"] == 29:
                if DEBUG:
                    print "--- rate limit exceeded, now sleep 5 minutes ---"
                time.sleep(5 * 60)
            return result
            # return r.text
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout, socket.timeout):
            try_num += 1


def get_user_info(username):
    url = API_REQUEST_URL % "user.getInfo"
    params = {'user': username}
    return mild_request(url, params)['user']


def get_user_friends(username, per_page=10, page=0):
    friends_dict = {}
    fetch_all = False

    if page == 0:
        page = 1
        fetch_all = True

    # make a request to get friends info
    url = API_REQUEST_URL % "user.getFriends"
    params = {'user': username, 'limit': per_page}

    while True:
        params['page'] = page
        if DEBUG:
            print '--- user=%s friends page=%d ---' % (username, page)
        obj = mild_request(url, params=params)['friends']
        if page == 1 and 'user' not in obj: # no friends
            return friends_dict
        else:
            attr = obj['@attr']
            total_friends = int(attr['total'])
            last_page = int(attr['totalPages'])
            if isinstance(obj['user'], dict):
                u = obj['user']
                friends_dict[u['name']] = u 
            else:
                for u in obj['user']:
                    friends_dict[u['name']] = u
            
            page += 1
            if len(friends_dict) > 1000 or not fetch_all or page > last_page:
                return friends_dict
                break



def handle_each_user(username):
    print "--- now for user=%s ---" % username
    friends_info_dict = get_user_friends(username)
    friends_list = map(str, friends_info_dict.keys())
    friends_set = set(friends_list)
    new_user = friends_set - USERS_POOL
    old_user = friends_set - new_user
    for u in old_user:
        del friends_info_dict[u]
    USERS_POOL.update(new_user)
    FIFO_QUEUE.extend(new_user)
    log_user_friends(username, friends_list,
            USER_FRIENDS_LOG_FILE)
    log_friends_info(friends_info_dict, USER_INFO_LOG_FILE)
    return new_user


# def get_recent_tracks(username, page=0, limit=10, extended=1):
    # url = API_REQUEST_URL % "user.getRecentTracks"
    # if page == 0:
        # page = 1
        # fetch_all = True
    # params = {'user': username, 'limit': limit, "page": page, "extended": extended}


def persistence():
    objs = {'pool': USERS_POOL, 'queue': FIFO_QUEUE}
    file_name = "data.pkl"
    f = open(file_name, "w")
    cPickle.dump(objs, f, 2)
    f.close()


def reproduct():
    queue = FIFO_QUEUE
    pool = USERS_POOL
    num_logged_info_users = len(USERS_POOL)
    while len(queue):
        if num_logged_info_users > 100000:
            print "The users is enough!"
            break
        head = queue.popleft()
        head = str(head)  # json.loads get unicode
        # log all new user info in friends and log all friends 
        num_logged_info_users += len(handle_each_user(head))
        persistence()
        if DEBUG:
            print "--- now have %d user_infos ---" % num_logged_info_users
    if DEBUG and not len(queue):
        print "--- converged!!! ---"
    if num_logged_info_users > 100000:
        persistence()


def log_user_info(user_info, userinfo_log_file):
    print >> userinfo_log_file, user_info

def log_user_friends(user, friends, friends_log_file):
    for f in friends:
        print >> friends_log_file, user, f

def log_friends_info(user_info_dict, user_info_log_file):
    for u in user_info_dict:
        print >> user_info_log_file, json.dumps(user_info_dict[u])

def get_listening_now_users(song_url):
    url = song_url + '+listeners'


if __name__ == "__main__":
    if  True:
        file_name = "data.pkl"
        USER_INFO_LOG_FILE = open("user_info.txt", "a")
        USER_FRIENDS_LOG_FILE = open("user_friends.txt", "a")

        if os.path.exists(file_name):
            # restore last run
            with open(file_name) as f:
                objs = cPickle.load(f)
                USERS_POOL = objs['pool']
                FIFO_QUEUE = objs['queue']
        else:
            pool = USERS_POOL
            queue = FIFO_QUEUE
            pool.add(seed_user)
            queue.append(seed_user)
        try:
            reproduct()
        except:
            USER_FRIENDS_LOG_FILE.flush()
            USER_INFO_LOG_FILE.flush()
            raise

