#!/bin/bash -l
module purge
module load PrgEnv-gnu
module load craype-haswell

old_dir=`pwd`
cd $PEGASUS_SCRATCH_DIR

cmd="srun -n 1 $PEGASUS_HOME/bin/pegasus-kickstart ${EXEC} $@"
echo ${cmd}
start=$SECONDS
${cmd}
end=$SECONDS
echo "Duration: $((end-start)) seconds."

cd $old_dir