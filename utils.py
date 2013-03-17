# -*- coding: utf-8 -*-
import gevent
from gevent import monkey; monkey.patch_all(thread=False, select=False)
import gevent.queue
from pyquery import PyQuery as pq
import requests
import socket
import time
import cPickle
from datetime import datetime
from collections import deque

API_REQUEST_URL = 'http://ws.audioscrobbler.com/2.0/?method=%s&api_key=9edee2e7f91969898fa60945cd818b55&format=json'
DEBUG = True
_Sessions = [requests.session() for i in range(10)]
_Queue = gevent.queue.Queue()
for s in _Sessions:
    _Queue.put(s)


def curren_time():
    return time.strftime("[%Y-%m-%d %H:%M:%S]", time.localtime())

def mild_request(url, params={}, timeout=5):
    session = _Queue.get()
    try_num = 1
    while True:
        try:
            # r = session.get(url, timeout=timeout, params=params)
            if DEBUG and try_num > 1:
                print "--- retry number = %d ---" % try_num
            to = gevent.Timeout(timeout)
            to.start()
            r = session.get(url, params=params)
            print curren_time(),
            print r.url
            break
        except gevent.timeout.Timeout as e:
            if e is to:
                if DEBUG and try_num > 10:
                    print "--- now give up ---"
                    return None
                try_num += 1
            else:
                raise # not my timeout
        except requests.ConnectionError: 
            print "--- url=%s and params=%s cause connection error ---" % (url, params)
            return None
        finally:
            to.cancel()
    _Queue.put(session)
    return r


def api_request(method, params={}):
    url = API_REQUEST_URL % method
    r =  mild_request(url, params)
    try:
        result = r.json()
        if "error" in result and result["error"] == 29:
            if DEBUG:
                print "--- rate limit exceeded, now sleep 5 minutes ---"
            gevent.sleep(5 * 60) # just breathe
            return api_request(method, params)
        else:
            return result # no error
    except ValueError:
        # error in json parse
        return None

def dom_request(url):
    text = mild_request(url).text
    dom = pq(text)
    return dom


def gevent_do(func, arg_list):
    assert _Queue.qsize() == 10
    jobs = [gevent.spawn(func, arg) for arg in arg_list]
    gevent.joinall(jobs)
    return [job.value for job in jobs]

class BatchRegulate(object):
    def __init__(self, func, arg_list, cap=10):
        self._func = func
        self._arg_list = arg_list # arg_list must be list
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
    
    def append(elem):
        if not self._finished:
            self._arg_list.append(elem)
        else:
            raise RuntimeError

    def extend(elems):
        if not self._finished:
            self._arg_list.extend(elem)
        else:
            raise RuntimeError

        
def save(filename, obj):
    with open(filename, "w") as f:
        cPickle.dump(obj, f, 2)
        f.flush()

def strptime(date_string, time_format="%d %b %Y, %H:%M"):
    date_string = str(date_string.strip())
    return datetime.strptime(date_string, time_format)


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
