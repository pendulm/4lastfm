# -*- coding: utf-8 -*-
from utils import api_request, gevent_do, save, is_recent_s
import gevent
import cPickle
import os.path

def get_top_tracks(saved_obj={}, limit=50):
    upper = 1000
    top_tracks = []
    service = "chart.getTopTracks"
    full = False
    count = 0
    save_file = "data/top_tracks.pkl"

    if 'page' in saved_obj:
        page = saved_obj['page']
    else:
        page = 1

    if 'tracks' in saved_obj:
        top_tracks = saved_obj['tracks']
    else:
        saved_obj['tracks'] = top_tracks

    params = {'page': page, 'limit': limit}
    if  len(top_tracks) >= upper:
        full = True

    while not full:
        obj = api_request(service, params)
        tracks = obj['tracks']['track']
        if isinstance(tracks, dict):
            info = get_track_fullinfo(tracks)
            top_tracks.append(info)
            print "--- len=%d ---" % len(top_tracks)
        elif isinstance(tracks, list):
            # for t in tracks:
                # info = get_track_fullinfo(t)
                # top_tracks.append(info)
                # print "--- len=%d ---" % len(top_tracks)
                # if  len(top_tracks) >= 1000:
                    # full = True
                    # break
            batch_result = gevent_do(get_track_fullinfo, tracks)
            if None in batch_result:
                page -= 1 # redo
            else:
                top_tracks.extend(batch_result)
                print "--- len=%d ---" % len(top_tracks)
        if  len(top_tracks) >= upper:
            full = True

        page += 1
        params['page'] = page
        saved_obj['page'] = page
        save(save_file, saved_obj)

    return top_tracks


def get_track_info(info, username=''):
    service = "track.getInfo"
    # params1 = {}
    # track = info['name']
    # artist = info['artist']['name']
    # params1['track'] = track
    # params1['artist'] = artist
    params = {'track': info['name'], 'artist': info['artist']['name']}

    # use_mbid = True
    # if 'mbid' in info and info['mbid']:
        # params2 = {'mbid': info['mbid']}
    # else:
        # use_mbid = False

    # params = params2 if use_mbid else params1

    result =  api_request(service, params)
    if not result or "error" in result:
        # no result
        # if use_mbid:
            # try another
            # result = api_request(service, params1)
            # if "error" not in result:
                # return result['track']
        print '--- get no infomation for track ---'
        return {}

    return result['track']

def get_track_shouts_num(info, limit=1, page=1):
    service = "track.getShouts"
    # params = {'limit': limit, 'page': page}

    # params1 = { 'track': info['name'], 'artist': info['artist']['name']}
    params = { 'limit': limit, 'page': page,
            'track': info['name'], 'artist': info['artist']['name']}

    # use_mbid = True
    # if 'mbid' in info and info['mbid']:
        # params2 = {'mbid': info['mbid']}
        # params2.update(params)
    # else:
        # use_mbid = False
        # params1.update(params)

    # param = params2 if use_mbid else params1
    while True:
        result =  api_request(service, params)
        if not result or "error" in result:
            print '--- get no shouts for track ---'
            return 0

        if 'shouts' in result:
            break
        elif 'status' in result and result['status'] == 'ok':
            gevent.sleep(0.5)
            pass # loop again!!!
        else:
            return 0
    if '@attr' in result['shouts']:
        return int(result['shouts']['@attr']['total'])
    elif 'total' in result['shouts']:
        # for some new track have no shouts
        return int(result['shouts']['total'])
    else:
        # just in case
        return 0




def get_track_fullinfo(info):
    full_info =  get_track_info(info)
    info.update(full_info)
    info['total_shouts'] = get_track_shouts_num(info)
    info['releasedate'] = get_track_date(info)
    return info

def get_track_date(info):
    service = "album.getInfo"
    params = {'artist': info['artist']['name']}
    if 'album' in info:
        params['album'] = info['album']['title']
    else:
        params['album'] = info['name']
    result =  api_request(service, params)
    if not result or "error" in result:
        print '--- get no shouts for track ---'
        return ''

    return result['album']['releasedate']

def filter_recent(tracks):
    return filter(lambda t: t["releasedate"].strip() and is_recent_s(t["releasedate"]),
            filter(None, tracks))

def get_filtered_top_tracks():
    save_file = "data/top_tracks.pkl"
    obj = {}
    # restore last progress
    if os.path.exists(save_file):
        f = open(save_file, "rb")
        obj = cPickle.load(f)
        f.close()

    failed_num = 0
    get_top_tracks(obj)
    save_file = "data/recent_tracks.pkl"
    recent_tracks = filter_recent(obj["tracks"])
    print "--- get %d valid recent tracks ---" % len(recent_tracks)
    save(save_file, recent_tracks)

def update_top_track(week):
    target_week_data = "data/" + ("week_%s/" % week) + "tracks.pkl"
    save_file = "data/tracks_info.pkl"
    update_info = []
    invalid_count = 0
    with open(save_file) as f:
        tracks = cPickle.load(f)
    for index, t in enumerate(tracks, start=1):
        result = get_track_fullinfo(t)
        print "--- update for track=%s(%d, %d) ---" % (t['name'], index, len(tracks))
        if result is None:
            invalid_count += 1
            print "--- get invalid track ---"
        update_info.append(result)

    print "--- get %d invalid tracks ---" % invalid_count
    print "--- save to file %s ---" % target_week_data
    save(target_week_data, update_info)



if __name__ == "__main__":
    if True:
        update_top_track(21)

