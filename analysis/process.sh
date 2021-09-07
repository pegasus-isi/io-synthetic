#!/bin/bash
LOG_DIR=./log
function generate_statistics {
    for d in ${LOG_DIR}/decaf-*/ ; do
        echo "$d"
        pegasus-statistics -s all ${d}
    done
}

function generate_cumulative_runtime {
    for d in ${LOG_DIR}/*/ ; do
        ctime=$(cat ${d}/statistics/breakdown.txt | grep -m1 keg | awk -F' ' '{print $7}')
        echo "$d ${ctime}"
    done
}

function generate_decaf_cumulative_runtime {
    for d in ${LOG_DIR}/decaf-*/ ; do
        ctime=$(cat ${d}/00/00/merge_cluster1.out | sed -n '3 p' | sed 's/.*is \([^ ]*\)[ ]*.*/\1/')
        echo "$d ${ctime}"
    done
}

function get_stats {
    # cat stat | grep $1 | awk -F' ' '{print $2}'
    cat decaf_stat | grep $1 | awk '{if($2!=""){count++;sum+=$2};y+=$2^2} END{sq=sqrt(y/NR-(sum/NR)^2);sq=sq?sq:0;print "mean = "sum/count ORS "std = ",sq}'
}

# generate_statistics
# generate_cumulative_runtime
opts=( "sleep" "sleep" )
sizes=( 1 2 4 8 16 )
for opt in "${opts[@]}"; do
    for size in "${sizes[@]}"; do
        echo ${size}g-${opt}
        get_stats ${size}g-${opt}
    done
done
# generate_decaf_cumulative_runtime