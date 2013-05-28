# -*- coding: utf-8 -*-
from collections import deque
from utils import api_request, dom_request, gevent_do, save, BatchRegulate
from pyquery import PyQuery as pq
import random
import json
import os.path
import cPickle
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
    result = api_request(method, params)
    if result is None or 'user' not in result:
        return None
    return result['user']


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
        if page == 1 and 'user' not in obj:  # no friends
            return (friends_dict, 0)
        else:
            attr = obj['@attr']
            total_friends = int(attr['total'])
            last_page = int(attr['totalPages'])
            if total_friends > 200:
                # pass this user
                # too many friends
                return None
            if isinstance(obj['user'], dict):
                u = obj['user']
                friends_dict[u['name']] = u
            else:
                for u in obj['user']:
                    friends_dict[u['name']] = u

            page += 1
            if len(friends_dict) > 1000 or not fetch_all or page > last_page:
                # just return the result
                return (friends_dict, total_friends)
                break


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
        friends_info = {k: v for k, v in friends_info.iteritems()
                        if k not in seed_users}
        if not friends_info:
            continue
        # select only one target
        # key = random.choice(friends_info.keys())
        # target_users_info.update({key: friends_info[key]})
        # select 6 friends
        if len(friends_info) >= 6:
            keys = random.sample(friends_info, 6)
            friends_info = {k: v for k, v in friends_info.iteritems()
                            if k in keys}
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
    name = user_info['name'].strip()
    if gender == "" or gender == "n":
        return False
    if age == "":
        return False

    # private listen or no history
    params = {'user': name, 'limit': 1, 'page': 1}
    method = "user.getRecentTracks"
    result = api_request(method, params)
    if result is None:
        return False

    # friends too much or something in friends fetching error
    result = get_user_friends(name, limit=1, page=1)
    if result is None:
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
        targets_info = {k: v for k, v in targets_info.iteritems()
                        if filter_target_info(v)}
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
        # targets_info = {k: v for k, v in targets_info.iteritems()
        # if k in keys}

    save(target_data, targets_info)
    print "--- total %d targets ---" % len(targets_info)
    save("data/target_friends.pkl", target_friends)
    count = sum(len(fr) for fr in target_friends.itervalues())
    print "--- all targets have %d friend counts ---" % count
    save("data/friends_info.pkl", friends_info)
    print "--- total %d friends ---" % len(friends_info)


def update_targets(week):
    target_week_data = "data/" + ("week_%s/" % week) + "target_users.pkl"
    invalid_week_data = "data/" + ("week_%s/" % week) + "invalid_users.pkl"
    update_info = {}
    invalid_users = []
    with open("data/target_users.pkl") as orig:
        targets = cPickle.load(orig)
        # for index, t in enumerate(targets, start=1):
            # print "--- get user=%s(%d:%d) ---" % (t, index, len(targets))
            # result = get_user_info(t)
            # if result is None:
                # print "--- user=%s get invalid info ---" % t
                # invalid_users.append(t)

            # update_info[t] = result
        from utils import iter_pool_do
        info_iter = iter_pool_do(get_user_info, targets.keys())
        index = 1
        for name, info in info_iter:
            print "--- get user=%s(%d:%d) ---" % (name, index, len(targets))
            if info is None:
                print "--- user=%s get invalid info ---" % name
                invalid_users.append(name)

            update_info[name] = info
            index += 1

    print "---- update to file %s %d targets---" % (
        target_week_data, len(update_info))
    save(target_week_data, update_info)
    print "---- update to file %s %d invalids---" % (
        invalid_week_data, len(invalid_users))
    save(invalid_week_data, invalid_users)
    return update_info


def get_random_target_from_seed(seed, num):
    limit = 50
    result = get_user_friends(seed, limit=limit, page=1)
    if result is None:
        return None
    info, total = result
    sample_num = min(num, len(info))
    select = random.sample(info, sample_num)
    return {k: v for k, v in info.iteritems() if k in select}


def get_n_valid_targets(n):
    target_data = "data/cut_target_users.pkl"
    seed_data = "data/seed_users.pkl"
    target_users = cPickle.load(open(target_data))
    seed_users = cPickle.load(open(seed_data))
    exclude = seed_users | set(target_users.keys())
    target_users = None
    count = 0
    new_targets_info = {}
    while count < n:
        seed = random.sample(seed_users, 1)[0]
        num = random.randint(1, 6)
        friends_info = get_random_target_from_seed(seed, num)
        if friends_info is None:
            continue
        for f in friends_info:
            if f not in exclude and filter_target_info(friends_info[f]):
                new_targets_info[f] = friends_info[f]
                count += 1
                print "--- get %d:%d user=%s ---" % (count, n, f)
                if count == n:
                    break

    return new_targets_info


if __name__ == "__main__":
    if True:
        # lonely_targets = []
        # friends_info = {}
        # target_friends = {}

        # extra_targets = list(cPickle.load(
            # open("data/target_users_need_get_friends.pkl")))
        # # result = pool_do(get_user_friends, extra_targets, cap=1)
        # # for (target, friends) in result.iteritems():
            # # if friends is None:
                # # lonely_targets.append(target)
                # # print "---%s is lonely ---" % target
            # # else:
                # # friends_dict, _ = friends
                # # friends_info.update(friends_dict)
                # # target_friends[target] = friends_dict.keys()
        # for target in extra_targets:
            # result = get_user_friends(target)
            # if result is None:
                # lonely_targets.append(target)
                # print "---%s is lonely ---" % target
            # else:
                # friends_dict, _ = result
                # friends_info.update(friends_dict)
                # target_friends[target] = friends_dict.keys()

        # save("lonely_targets.pkl", lonely_targets)
        # save("new_friends_info.pkl", friends_info)
        # save("new_target_friends.pkl", target_friends)
        update_targets(21)
