select 12, userID, username, base1.trackID, trackname, base2.track_stat, base2.artist_stat, 0, 
       trackPlayCnt, Shouts, listeners, artist, duration, releasedate, ArtistID, ArtistYears, Country, LastAlbum, LatestRelease, ArtistListenersAllTime, ArtistScrobblesAllTime, shouts,
       age, country, registered, gender, userPlayCount, listenedArtist, FriendNum, userType
    from
    (select * from  (select * from tracks left join artists on tracks.artist == artists.name), targets) as base1 
    left join 
    (select t1.user, t1.trackID, t1.stat as track_stat,
                                t2.stat as artist_stat
        from targets_tracks_stat as t1, targets_artists_stat as t2
        where (t1.user == t2.user and t1.artist == t2.artist)) as base2
    on (base1.username == base2.user and base1.trackID == base2.trackID);
    
