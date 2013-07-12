function perror() {
    local msg=$1
    echo "$msg" 1>&2
    exit 1
}

week=$1
lastweek=$(( week-1 ))
[[ -z $week ]] && perror "specify week!"

week_dir="../data/week_${week}"
week_history="${week_dir}/listened_history_week_${lastweek}_${week}.db"
history_db="${week_dir}/listen_history.db"
user_art_stat="${week_dir}/user_artist_stat.txt"
friend_week_history="${week_dir}/friend_listeners.db"
week_tracks_info="${week_dir}/tracks.pkl"
week_targets_info="${week_dir}/target_users.pkl"

[[ -e $week_history && -e $history_db ]] || perror "require database!"

# update week history
echo '.dump history' | \
    sqlite3 $week_history | \
    sqlite3 $history_db
echo "update week history completed"

# update week track
[[ -e $week_tracks_info ]] && python update_table.py $week track && echo "update week tracks info completed"

# update user artist statistic
echo "select user, count(distinct artist), count(*) 
          from history group by user;" | sqlite3 $history_db > $user_art_stat
echo "update user artists statistics"

# update week targets
[[ -e $week_targets_info ]] && python update_table.py $week target && echo "update week targets info completed"

# finally output the map
[[ -e $friend_week_history ]] || perror "no friends week history!"
echo "attach database '${friend_week_history}' as friend_listeners;
select ${week}, userID, username, base1.trackID, trackname, base2.track_stat, t3.FriendsListenerNum, base2.artist_stat, 0, 
       trackPlayCnt, Shouts, listeners, artist, duration, releasedate, ArtistID, ArtistYears, Country, LastAlbum, LatestRelease, ArtistListenersAllTime, ArtistScrobblesAllTime, shouts,
       age, country, registered, gender, userPlayCount, listenedArtist, FriendNum, userType
    from
    (
        (select * from  (select * from tracks left join artists on tracks.artist == artists.name), targets) as base1 
        left join 
        (select t1.user, t1.trackID, t1.stat as track_stat,
                                    t2.stat as artist_stat
            from targets_tracks_stat as t1, targets_artists_stat as t2
            where (t1.user == t2.user and t1.artist == t2.artist)) as base2
        on (base1.username == base2.user and base1.trackID == base2.trackID)
    ) as step1
    join
    friend_listeners.friend_listeners as t3
    on (step1.userID == t3.target_id  and step1.trackID == t3.track_id);" | \
sqlite3 "${week_dir}/listen_history.db"  | \
awk 'BEGIN { FS="|"; OFS="|"}; { if ($6 == "") {$6 = "0"} if ($8 == "") {$8 = "0"} print }' > "${week_dir}/week_${week}.map"
echo "exported map!"
