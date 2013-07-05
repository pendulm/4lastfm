week=$1
[[ -z $week ]] && { echo "specify week!" 1>&2; exit 1; }
week_dir="../data/week_${week}"

echo "attach database '${week_dir}/friend_listeners.db' as friend_listeners;
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
    on (step1.userID == t3.target_id  and step1.trackID == t3.track_id);" | sqlite3 "${week_dir}/listen_history.db"  | awk 'BEGIN { FS="|"; OFS="|"}; { if ($6 == "") {$6 = "0"} if ($8 == "") {$8 = "0"} print }' > "${week_dir}/week_${week}.map"
