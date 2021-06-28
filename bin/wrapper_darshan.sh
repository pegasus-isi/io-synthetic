#!/bin/bash -l
module purge
module load PrgEnv-gnu
module load craype-haswell
module unload darshan
export PATH=$HOME/software/install/darshan-nonmpi/bin:$SCRATCH/software/install/darshan-utils/bin:$PATH
export DARSHAN_EXCLUDE_DIRS="$DARSHAN_EXCLUDE_DIRS,$SCRATCH/.local/,$HOME/.local/,/global/u2/l/$USER/.local/,/usr/common/software/,/global/common/cori_cle7/software/,/opt/,/etc/,/proc/,/sys/,$HOME/.cache/"
export DLOGPATH=$PEGASUS_SCRATCH_DIR/${LOG_FN}

old_dir=`pwd`
cd $PEGASUS_SCRATCH_DIR

cmd="srun -n 1 --export=ALL,DARSHAN_ENABLE_NONMPI=1,LD_PRELOAD=$HOME/software/install/darshan-nonmpi/lib/libdarshan.so ${PEGASUS_HOME}/bin/pegasus-keg $@"
echo ${cmd}
start=$SECONDS
${cmd}
end=$SECONDS
echo "Duration: $((end-start)) seconds."

cd $old_dir