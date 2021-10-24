#!/bin/bash -l
module purge
module load PrgEnv-gnu
module load craype-haswell
module unload darshan
export PATH=${USER_HOME}/software/install/darshan-nonmpi/bin:${USER_HOME}/software/install/darshan-utils/bin:$PATH
export DARSHAN_EXCLUDE_DIRS="$DARSHAN_EXCLUDE_DIRS,$SCRATCH/.local/,${USER_HOME}/.local/,/global/u2/l/$USER/.local/,/usr/common/software/,/global/common/cori_cle7/software/,/opt/,/etc/,/proc/,/sys/,${USER_HOME}/.cache/"
export DLOGPATH=${USER_HOME}/darshan
echo $DARSHAN_EXCLUDE_DIRS
echo $DLOGPATH

DARSHAN="--export=ALL,DARSHAN_ENABLE_NONMPI=1,LD_PRELOAD=${USER_HOME}/software/install/darshan-nonmpi/lib/libdarshan.so"

old_dir=`pwd`
cd $PEGASUS_SCRATCH_DIR

cmd="srun -n 1 ${DARSHAN} $PEGASUS_HOME/bin/pegasus-kickstart ${EXEC} $@"
echo ${cmd}
start=$SECONDS
${cmd}
end=$SECONDS
echo "Duration: $((end-start)) seconds."

cd $old_dir