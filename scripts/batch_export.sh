start_week=$1
end_week=$2
week_dir_prefix="../data/week_"

[[ -n $start_week && -n $end_week ]] || { echo "specify week range" 1>&2; exit 1; }

for (( current_week=$start_week; current_week <= $end_week; ++current_week ))
do
    echo "process for week $current_week"
    bash export_map.sh $current_week
    ( cd ../data; /bin/mv "week_${current_week}/listen_history.db" "week_$(( current_week+1 ))/listen_history.db" )
done
