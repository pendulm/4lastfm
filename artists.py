# coding: utf-8
import cPickle as pickle
import sqlite3
import os.path
from utils import save

conn = sqlite3.connect('data/listen_history.db')
cursor = conn.cursor()

def create_artists_table():
    cursor.executescript("""
        create table if not exists artists (
            name,
            url,
            mbid,
            first_release,
            last_realease
        );
    """)
    conn.commit()

def fill_artists_table(info):
    art_list = info.itervalues()
    for art in art_list:
        tu = (art['name'], art['url'], art['mbid'])
        cursor.execute("insert into artists (name, url, mbid) values (?, ?, ?);", tu)
    conn.commit()

def collect_artist_from_tracks(tracks):
    artists_info = {}
    for t in tracks:
        art = t['artist']
        name = art['name']
        if name not in artists_info:
            artists_info[name] = art

    return artists_info

def get_top_tracks(filename="data/recent_tracks.pkl"):
    return pickle.load(open(filename))

def save_artists_info(info):
    save("data/artists_info.pkl", info)

def get_artists_info(filename="data/artists_info.pkl"):
    if os.path.exists(filename):
        return pickle.load(open(filename))
    top_tracks = get_top_tracks()
    artists_info = collect_artist_from_tracks(top_tracks)
    return artists_info

def save_artists_from_tracks():
    artists_info = get_artists_info()
    save_artists_info(artists_info)


def first_album_time(name):
    pass


if __name__ == '__main__':
    # save_artists_from_tracks()
    create_artists_table()
    art_info = get_artists_info()
    fill_artists_table(art_info)
