# -*- coding: utf-8 -*-
from utils import mild_request, gevent_do, sleep
import cPickle
import os.path

def get_top_tracks(saved_obj={}, limit=10):
    upper = 1000
    top_tracks = []
    service = "chart.getTopTracks"
    full = False
    count = 0
    save_file = "for_track.pkl"

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
        obj = mild_request(service, params)
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

    result =  mild_request(service, params)
    if not result or "error" in result:
        # no result
        # if use_mbid:
            # try another
            # result = mild_request(service, params1)
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
        result =  mild_request(service, params)
        if not result or "error" in result:
            print '--- get no shouts for track ---'
            return 0

        if 'shouts' in result:
            break
        elif 'status' in result and result['status'] == 'ok':
            sleep(0.5)
            pass # loop again!!!
        else:
            return 0
    return int(result['shouts']['@attr']['total'])

    


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
    result =  mild_request(service, params)
    if not result or "error" in result:
        print '--- get no shouts for track ---'
        return ''

    return result['album']['releasedate']


def save(filename, obj):
    with open(filename, "w") as f:
        cPickle.dump(obj, f, 2)
        f.flush()

if __name__ == "__main__":
    save_file = "for_track.pkl"
    obj = {}
    if os.path.exists(save_file):
        f = open(save_file, "rb")
        obj = cPickle.load(f)
        f.close()
    failed_num = 0
    get_top_tracks(obj)
