# -*- coding: utf-8 -*-
import gevent
from gevent import monkey; monkey.patch_all(thread=False, select=False)
import gevent.queue
from pyquery import PyQuery as pq
import requests
import socket
import time
import cPickle
from time import sleep
from datetime import datetime

API_REQUEST_URL = 'http://ws.audioscrobbler.com/2.0/?method=%s&api_key=9edee2e7f91969898fa60945cd818b55&format=json'
DEBUG = True
_Sessions = [requests.session() for i in range(10)]
_Queue = gevent.queue.Queue()
for s in _Sessions:
    _Queue.put(s)


def curren_time():
    return time.strftime("[%Y-%m-%d %H:%M:%S]", time.localtime())

def mild_request(url, params={}, timeout=10):
    session = _Queue.get()
    try_num = 1
    while True:
        try:
            r = session.get(url, timeout=timeout, params=params)
            print curren_time(),
            print r.url
            if DEBUG and try_num > 1:
                print "--- retry number = %d ---" % try_num
            break
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout, socket.timeout):
            try_num += 1
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
            time.sleep(5 * 60) # just breathe
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
    jobs = [gevent.spawn(func, arg) for arg in arg_list]
    gevent.joinall(jobs)
    return [job.value for job in jobs]


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
