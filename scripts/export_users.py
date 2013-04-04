import csv
import json
import cPickle
field_name = ['name', 'realname', 'url', 'country', 'age', 'gender',
      'bootstrap', 'playlists', 'registered', 'subscriber',
      'playcount', 'type', 'id', 'listened_record', 'listened_artists', 'friends_num']

fi = open("../data/final_target_user.pkl", "r")
users = cPickle.load(fi)
male = 0
female = 0
unknown = 0

statf = open("../data/user_artist_stat.txt")
extra_dict = {}
for l in statf:
    l = l.strip()
    col = l.split('|')
    extra_dict[col[0]] = tuple(col[1:])

friends = cPickle.load(open("../data/final_target_friends.pkl"))

with open('../results/users_info.csv', 'wb') as csvfile:
    w = csv.DictWriter(csvfile, field_name, extrasaction='ignore')
    first = dict((f, f) for f in field_name)
    w.writerow(first)
    for (n, u) in users.iteritems():
        for i in u:
            value = u[i]
            if isinstance(value, unicode):
                u[i] = value.encode('utf-8')
        if u['gender'] == 'm':
            male += 1
        elif u['gender'] == 'f':
            female += 1
        else:
            unknown += 1
        u['listened_artists'] = extra_dict[n][0]
        u['listened_record'] = extra_dict[n][1]
        u['friends_num'] = len(friends[n])
        w.writerow(u)
print "male=%d female=%d unknown=%d" % (male, female, unknown)
