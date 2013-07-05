# -*- coding: utf-8 -*-
import gevent
from gevent import monkey; monkey.patch_all(thread=False, select=False)
import gevent.queue
from pyquery import PyQuery as pq
import requests
import time
import cPickle
import random
import sqlite3
from datetime import datetime, timedelta
from itertools import cycle
from collections import deque
from config import lastfmtoken

API_REQUEST_URL = 'http://ws.audioscrobbler.com/2.0/?method=%%s\
&api_key=%s&format=json'
DEBUG = True
_Sessions = [requests.session() for i in range(10)]
_Queue = gevent.queue.Queue()
for s in _Sessions:
    _Queue.put(s)


def request_base_url_generator():
    base_urls = [API_REQUEST_URL % t for t in lastfmtoken]
    urls_gen = cycle(base_urls)
    for t in urls_gen:
        yield t

def get_request_base_url():
    if not hasattr(get_request_base_url, "gen"):
        get_request_base_url.gen = request_base_url_generator()
    gen = get_request_base_url.gen
    return next(gen)

def curren_time():
    return time.strftime("[%Y-%m-%d %H:%M:%S]", time.localtime())


def mild_request(url, params={}, timeout=5, max_retry=10):
    session = _Queue.get()
    try_num = 1
    while True:
        try:
            # r = session.get(url, timeout=timeout, params=params)
            if DEBUG and try_num > 1:
                print "--- %s number = %s ---" % (
                        Color.emphasise("retry"),
                        Color.emphasise(try_num))
            to = gevent.Timeout(timeout)
            to.start()
            r = session.get(url, params=params)
            print curren_time(),
            print r.url
            break
        except gevent.timeout.Timeout as e:
            if e is to:
                if DEBUG and try_num > max_retry:
                    print "--- %s ---" % Color.fail("now give up")
                    _Queue.put(session)
                    return None
                try_num += 1
                gevent.sleep(random.uniform(0, 2))
            else:
                raise  # not my timeout
        except requests.ConnectionError:
            print "--- url=%s and params=%s cause connection %s ---" % (
                Color.emphasise(url),
                Color.emphasise(params),
                Color.fail("error"))
            _Queue.put(session)
            return None
        finally:
            to.cancel()
    _Queue.put(session)
    return r

def request_url(method, params={}):
    url = get_request_base_url() % method
    req = requests.Request()
    req.method = 'GET'
    req.url = url
    req.params = params
    prep = req.prepare()
    return prep.url


def api_request(method, params={}):
    url = get_request_base_url() % method
    r = mild_request(url, params)
    if r is None:
        return None
    try:
        result = r.json()
        if "error" in result:
            if result["error"] == 29:
                if DEBUG:
                    print "--- %s, now sleep 5 seconds ---" % (
                            Color.warn("rate limit exceeded"),)
                gevent.sleep(5)  # just breathe
                # gevent.sleep(5 * 60) # just breathe
                return api_request(method, params)
            else:
                # error = 4
                # error = 6
                # ...
                return None
        else:
            return result  # no error
    except ValueError:
        # error in json parse
        return None


def dom_request(url):
    r = mild_request(url)
    if r is None:
        return None
    text = r.text
    dom = pq(text)
    return dom


def gevent_do(func, arg_list):
    assert _Queue.qsize() == 10
    jobs = [gevent.spawn(func, arg) for arg in arg_list]
    gevent.joinall(jobs)
    return [job.value for job in jobs]


def pool_do(func, arg_list, cap=5):
    pool_queue = gevent.queue.Queue()
    result_dict = {}

    def wrap1(arg, token):
        result = func(arg)
        result_dict[arg] = result
        # print "arg %s finished" % arg
        pool_queue.put(token)

    def wrap(arg):
        # print "arg %s stared" % arg
        token = pool_queue.get()
        task = gevent.spawn(wrap1, arg, token)
        return task

    for i in range(cap):
        # initialize task pool
        pool_queue.put(i)

    for arg in arg_list:
        task = wrap(arg)

    # if arg_list is empty this will raise UnBound
    if not task.ready():
        # at most one task remain
        task.join()

    return result_dict


def iter_pool_do(func, arg_list, cap=5):
    # well, i'm not sure there isn't bug in this...
    pool_queue = gevent.queue.Queue(maxsize=cap)
    seat_avail = cap
    yield_cnt = 0
    dispathed = 0

    def wrap(arg):
        pool_queue.put([arg, func(arg)])

    while dispathed != len(arg_list):
        if seat_avail > 0:
            seat_avail -= 1
            gevent.spawn(wrap, arg_list[dispathed])
            dispathed += 1
        else:
            yield pool_queue.get()
            seat_avail += 1
            yield_cnt += 1

    while yield_cnt != len(arg_list):
        yield pool_queue.get()
        yield_cnt += 1


# def test_pool_do(arg):
    # # gevent.sleep(1)
    # gevent.sleep(random.randint(1, 3))
    # return arg


class BatchRegulate(object):
    def __init__(self, func, arg_list, cap=10):
        self._func = func
        self._arg_list = arg_list  # arg_list must be list
        self._index = 0
        self._finished = False
        self._result = deque()
        if cap > 10 or cap < 1:
            self._cap = 10
        else:
            self._cap = cap

    def __iter__(self):
        return self

    def next(self):
        index = self._index
        cap = self._cap
        if len(self._result):
            return self._result.popleft()
        elif index + cap <= len(self._arg_list):
            result = gevent_do(self._func, self._arg_list[index: index+cap])
            self._index += cap
            self._result.extend(result)
            return self._result.popleft()
        elif index < len(self._arg_list):
            result = gevent_do(self._func, self._arg_list[index:])
            self._index = len(self._arg_list)
            self._result.extend(result)
            return self._result.popleft()
        else:
            self._finished = True
            raise StopIteration

    def append(self, elem):
        if not self._finished:
            self._arg_list.append(elem)
        else:
            raise RuntimeError

    def extend(self, elems):
        if not self._finished:
            self._arg_list.extend(elems)
        else:
            raise RuntimeError


def save(filename, obj):
    with open(filename, "w") as f:
        cPickle.dump(obj, f, 2)
        f.flush()


def strptime(date_string, time_format="%d %b %Y, %H:%M"):
    date_string = str(date_string.strip())
    return datetime.strptime(date_string, time_format)


def timestamp(date_string):
    dt = strptime(date_string)
    epoch = datetime.fromtimestamp(0)
    seconds = int((dt - epoch).total_seconds())
    return seconds


def get_n_months_ago(dt, n):
    year = dt.year
    month = dt.month
    day = dt.day
    if month - n <= 0:
        year -= 1
    month = (month - n) % 12
    if month == 0:
        month = 12
    return datetime(year, month, day)


def is_recent(dt):
    now = datetime.now()
    six_months_ago = get_n_months_ago(now, 6)
    if dt > six_months_ago:
        return True
    return False


def is_recent_s(date_string):
    return is_recent(strptime(date_string))


def timestamp_of_nth_week(week, year=None):
    if year is None:
        year = datetime.now().year
    first_day_of_year = datetime(year, 1, 1)
    nth_day_of_week = first_day_of_year.weekday()
    first_day_of_first_week = (first_day_of_year -
                               timedelta(days=nth_day_of_week))
    td = timedelta(weeks=week-1)
    day_of_week = first_day_of_first_week + td
    epoch = datetime.fromtimestamp(0)
    seconds = int((day_of_week - epoch).total_seconds())
    return seconds

def get_track_releasetime(track):
    return timestamp(track['releasedate'])

class Color(object):
    END = '\033[0m'
    RED = '\033[1;31m'
    GREEN = '\033[1;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[1;34m'

    @classmethod
    def fail(cls, msg):
        return "%s%s%s" % (cls.RED, msg, cls.END)

    @classmethod
    def warn(cls, msg):
        return "%s%s%s" % (cls.YELLOW, msg, cls.END)

    @classmethod
    def ok(cls, msg):
        return "%s%s%s" % (cls.GREEN, msg, cls.END)

    @classmethod
    def emphasise(cls, msg):
        return "%s%s%s" % (cls.BLUE, msg, cls.END)


class DBWrapper(object):
    def __init__(self, dbfile):
        self.mark = 1
        self.count = 1
        self.conn = sqlite3.connect(dbfile)
        self.cur = self.conn.cursor()

    def executescript(self, sql):
        self.cur.executescript(sql)
        self.mark = 1
        self.count = 1

    def execute(self, sql, tuple_val=None):
        if tuple_val:
            self.cur.execute(sql, tuple_val)
        else:
            self.cur.execute(sql)

        if self.mark == 2000:
            self.commit()
            self.mark = 1
            # print("%d" % self.count)
        else:
            self.mark += 1
        self.count += 1
        return self.cur

    def executemany(self, sql, lst):
        self.cur.executemany(sql, lst)
        self.count += len(lst)
        self.mark += len(lst)
        if self.mark >= 2000:
            self.commit()
            self.mark = 1
        return self.cur

    def commit(self):
        self.conn.commit()

def simple_cache(func):
    # python3 use nonlocal
    cache_container = []
    def inner(*args, **kwargs):
        if len(cache_container) == 0:
            result = func(*args, **kwargs)
            cache_container.append(result)
        else:
            result = cache_container[0]
        return result
    return inner
