import csv
import json
import cPickle
import collections

listened_stat_dict = {}
user_average = {}
target_users_info = cPickle.load(open("../data/final_target_user.pkl"))
target_users = target_users_info.keys()
artists_info = cPickle.load(open("../data/artists_info.pkl"))
artists = artists_info.keys()


for u in target_users:
    listened_stat_dict[u] = collections.defaultdict(int)
    user_average[u] = 0

for l in open("../data/user_listened_stat.txt"):
    l = l.strip()
    name, artist, cnt = l.split('|')
    listened_stat_dict[name][artist] = int(cnt)


for l in open("../data/user_artist_stat.txt"):
    l = l.strip()
    name, artist_num, total = l.split('|')
    user_average[name] = float(total) / float(artist_num)
field_name = ['user', 'artist', 'preference']
with open('../results/preference.csv', 'wb') as csvfile:
    w = csv.DictWriter(csvfile, field_name, extrasaction='ignore')
    first = dict((f, f) for f in field_name)
    w.writerow(first)
    for u in target_users:
        for art in artists:
            new_dict = {}
            new_dict['user'] = u
            new_dict['artist'] = art
            new_dict['preference'] = listened_stat_dict[u][art] / user_average[u]
            w.writerow(new_dict)

