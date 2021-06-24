#!/bin/bash -l
module purge
module load PrgEnv-gnu
module load craype-haswell

old_dir=`pwd`
cd $PEGASUS_SCRATCH_DIR

start=$SECONDS
srun -n 1 ${PEGASUS_HOME}/bin/pegasus-keg $@
end=$SECONDS
echo "Duration: $((end-start)) seconds."

cd $old_dir