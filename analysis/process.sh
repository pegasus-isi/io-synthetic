#!/bin/bash
LOG_DIR=./log
stat_file="summary.csv"
function generate_statistics {
    for d in ${LOG_DIR}/decaf-*/ ; do
        echo "$d"
        pegasus-statistics -s all ${d}
    done
}

function rename_dir {
    for d in ${LOG_DIR}/16g-*/ ; do
        base_name=$(basename $d)
        dir_name=$(dirname $d)
        echo "$d ${base_name} ${dir_name}"
        mv $d ${dir_name}/pegasus-${base_name}
    done 
}

function generate_pegasus_runtime {
    # for d in ${LOG_DIR}/pegasus-*/ ; do
        d=$1
        ctime=$(cat ${d}/statistics/breakdown.txt | grep -m1 keg | awk -F' ' '{print $7}')
        echo "${ctime}"
    # done
}

function generate_decaf_runtime {
    # for d in ${LOG_DIR}/decaf-*/ ; do
        d=$1
        ctime=$(cat ${d}/00/00/merge_cluster1.out | sed -n '3 p' | sed 's/.*is \([^ ]*\)[ ]*.*/\1/')
        echo "${ctime}"
    # done
}

function generate_pmc_runtime {
    # for d in ${LOG_DIR}/pmc-*/ ; do
        d=$1
        ctime=$(cat ${d}/00/00/merge_cluster1.err.* | grep "Wall time" | sed 's/.*Wall time: \([^ ]*\).*/\1/')
        echo "${ctime}"
    # done
}
function get_stats {
    # cat stat | grep $1 | awk -F' ' '{print $2}'
    cat decaf_stat | grep $1 | awk '{if($2!=""){count++;sum+=$2};y+=$2^2} END{sq=sqrt(y/NR-(sum/NR)^2);sq=sq?sq:0;print "mean = "sum/count ORS "std = ",sq}'
}

function generate_stats {
    echo "Scenario,Data size,Type,Makespan"
    for dir in ${LOG_DIR}/*/; do
        dir_name=$(basename $dir)
        parts=(${dir_name//-/ })
        scenario=${parts[0]}
        type=${parts[2]}
        size=${parts[1]}
        # echo "${scenario} ${size} ${type}"
        case $scenario in
            "pegasus")
                echo "Pegasus only,${size},${type},$(generate_pegasus_runtime $dir)"
                ;;
            "decaf")
                echo "Pegasus + Decaf,${size},${type},$(generate_decaf_runtime $dir)"
                ;;
            "pmc")
                echo "PMC,${size},${type},$(generate_pmc_runtime $dir)"
                ;;
            *)
                echo "Scenario should be either pegasus, decaf or pmc"
                ;;
        esac
    done
}

# generate_statistics
# generate_pegasus_runtime
# opts=( "sleep" "sleep" )
# sizes=( 1 2 4 8 16 )
# for opt in "${opts[@]}"; do
#     for size in "${sizes[@]}"; do
#         echo ${size}g-${opt}
#         get_stats ${size}g-${opt}
#     done
# done
# generate_decaf_runtime
# generate_pmc_runtime
generate_stats > ${stat_file}