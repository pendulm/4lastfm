# -*- coding: utf-8 -*-
import gevent
from gevent import monkey; monkey.patch_all(thread=False, select=False)
import requests
import socket
import time
from time import sleep

API_REQUEST_URL = 'http://ws.audioscrobbler.com/2.0/?method=%s&api_key=9edee2e7f91969898fa60945cd818b55&format=json'
DEBUG = True
session = requests.session()


def mild_request(method, params={}, timeout=10):
    try_num = 1
    url = API_REQUEST_URL % method
    while True:
        try:
            r = session.get(url, timeout=timeout, params=params)
            print r.url
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
        except ValueError:
            return None

def gevent_do(func, arg_list):
    jobs = [gevent.spawn(func, arg) for arg in arg_list]
    gevent.joinall(jobs, timeout=20)
    return [job.value for job in jobs]
