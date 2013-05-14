#! /bin/bash
python add_track_release.py
awk -f cal_user_left_time_range.awk ../data/friends_and_tracks_and_timestamp.txt > ../data/friends_left_time_range.txt
sqlite3 ../data/friends_listened.db 'select friendname, max(timestamp) from playcount_and_love group by friendname;' > ../data/friends_right_time_range.txt
join -t '|' ../data/friends_{left, right}_time_range.txt > ../data/friends_time_range.txt
