#!/bin/bash
function generate_statistics {
    for d in */ ; do
        echo "$d"
        pegasus-statistics -s all ${d}
    done
}

function generate_cumulative_runtime {
    for d in */ ; do
        ctime=$(cat ${d}/statistics/breakdown.txt | grep -m1 keg | awk -F' ' '{print $7}')
        echo "$d ${ctime}"
    done
}

function get_stats {
    # cat stat | grep $1 | awk -F' ' '{print $2}'
    cat stat | grep $1 | awk '{if($2!=""){count++;sum+=$2};y+=$2^2} END{sq=sqrt(y/NR-(sum/NR)^2);sq=sq?sq:0;print "mean = "sum/count ORS "std = ",sq}'
}

opts=( "nosleep" "sleep" )
sizes=( 1 2 4 8 )
for opt in "${opts[@]}"; do
    for size in "${sizes[@]}"; do
        echo ${size}g-${opt}
        get_stats ${size}g-${opt}
    done
done

