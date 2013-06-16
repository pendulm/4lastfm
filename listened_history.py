# -*- coding: utf-8 -*-
import requests
import sqlite3
from utils import mild_request, api_request, dom_request, gevent_do
from utils import save, BatchRegulate, pool_do, timestamp_of_nth_week
from utils import iter_pool_do
from friends import History, get_targets, restore_from_db, dispatch_one_user
from pyquery import PyQuery as pq
import random
import json
import sys
import os.path
import socket
import cPickle
import time

def prepare_history_db(start_week, end_week):
    conn = sqlite3.connect('data/week_%s/listened_history_week_%s_%s.db' %
            (end_week, start_week, end_week))
    cursor = conn.cursor()
    cursor.executescript("""
        create table if not exists history (
            user,
            track,
            artist,
            streamable,
            album,
            url,
            loved,
            datetime
        );
        create table if not exists meta_info (
            name,
            left_time,
            right_time,
            target
        );
    """)
    cursor.execute("select count(*) from meta_info;")
    count1 = cursor.fetchone()[0]
    cursor.execute("select count(*) from history;")
    count2 = cursor.fetchone()[0]
    # no record, it means run from scratch
    if count1 == 0 and count2 == 0:
        left_time = timestamp_of_nth_week(start_week)
        right_time = timestamp_of_nth_week(end_week)
        targets = get_targets()
        for target_name in targets:
            cursor.execute("insert into meta_info values (?, ?, ?, ?)",
                               (target_name, left_time, right_time, 1))
        conn.commit()

    History.conn = conn
    History.cursor = cursor
    return (cursor, conn)


def count_total_record(start_week, end_week):
    cursor, conn = prepare_history_db(start_week, end_week)
    users_param = restore_from_db(cursor)
    total_record_count = 0
    user_count = len(users_param)
    user_index = 0
    bobj = BatchRegulate(lambda param: History(*param).get_count(), users_param, 5)
    for user in users_param:
        user_index += 1
        n = next(bobj)
        if n is None:
            n = 0
        total_record_count += n
        print "count (%d/%d) users=%s listened=%d --- totals listened count = %d ---" % (
                user_index, user_count, user[0], n, total_record_count)
    return total_record_count


def get_week_range_history(start_week, end_week):
    cursor, conn = prepare_history_db(start_week, end_week)
    users_param = restore_from_db(cursor)
    History.total_user = len(users_param)
    users_with_index = list(enumerate(users_param, start=1))
    gen = iter_pool_do(dispatch_one_user, users_with_index, cap=4)
    for g in gen:
        pass


def update_targets_history(week):
    # History.debug = True
    History.per_page = 30
    LOG_FILE = "data/week_%s/targets_listend_history_week_%s_%s.log" % (
               week, week-1, week)
    History.logf = open(LOG_FILE, "a")
    get_week_range_history(week-1, week)
    History.logf.flush()



if __name__ == "__main__":
    update_targets_history(11)
