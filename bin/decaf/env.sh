#!/bin/bash
# Loaded Modules
module purge
unset LD_LIBRARY_PATH
module load PrgEnv-gnu
module load craype-haswell
module load cray-mpich
module load python
module load cmake
module load boost/1.69.0

# Decaf
export DECAF_PREFIX="/global/cfs/cdirs/m2187/pegasus-decaf/io-synthetic/bin/decaf"
export LD_LIBRARY_PATH="${DECAF_PREFIX}/lib/:$LD_LIBRARY_PATH"

