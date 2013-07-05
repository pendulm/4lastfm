select user, count(distinct artist), count(*) from history group by user;
